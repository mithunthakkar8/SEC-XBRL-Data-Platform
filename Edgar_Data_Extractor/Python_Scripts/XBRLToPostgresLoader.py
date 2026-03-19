import mmap
from arelle import Cntlr, FileSource
from urllib.parse import urljoin
import logging
import re
from collections import defaultdict
import psycopg2
import sys
from psycopg2.extras import DictCursor
from dateutil import parser
import hashlib
from psycopg2.extras import execute_batch
from Helper_Functions import extract_year, get_lei_by_name, get_iso_code, get_ticker_from_cik, query_yahoo, get_cleaned_value

class XBRLToPostgresLoader:
    def __init__(self, log_file='xbrl_to_postgres_loader.log', db_config=None):
        self.cntlr = Cntlr.Cntlr(logFileName=log_file)
        self.modelXbrl = None
        self.filing_id = None
        
        # Initialize logger
        self.logger = logging.getLogger("xbrl_to_postgres_loader")
        self.logger.setLevel(logging.DEBUG)  # Set the logging level

        # FileHandler for logging to a file
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        # StreamHandler for logging to console (stdout)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))

        # Add both handlers to the logger
        if not self.logger.handlers:
            self.logger.addHandler(file_handler)
            self.logger.addHandler(stream_handler)
        
        # Database configuration
        self.db_config = db_config or {
            'dbname': 'xbrl_data',
            'user': 'postgres',
            'password': 'postgres',
            'host': 'localhost',
            'port': '5432'
        }
        self.conn = None
    

    def _get_db_connection(self):
        """Get a database connection, reconnecting if necessary"""
        try:
            if self.conn and not self.conn.closed:
                return self.conn
            self.conn = psycopg2.connect(**self.db_config)
            return self.conn
        except Exception as e:
            self.logger.error(f"Error connecting to database: {str(e)}")
            raise
    
    def load_metadata(self, submission_file_path: str):
        """
        Extract metadata from SEC submission file and store in appropriate tables.
        Inserts industry first, then company, then filing.
        Skips existing companies and only inserts filing data for them.
        Returns company_id if successful, None otherwise.
        """
        conn = None
        try:
            self.logger.info("Starting company metadata extraction process.")

            # Step 1: Extract base metadata from submission file
            self.logger.debug(f"Extracting submission metadata from: {submission_file_path}")
            extracted_data = self._extract_submission_metadata(submission_file_path)

            company_name = extracted_data['company_name']
            self.logger.debug(f"Normalized company name: {company_name}")

            conn = self._get_db_connection()
            self.logger.debug("Database connection established.")

            with conn.cursor(cursor_factory=DictCursor) as cursor:
                self.logger.debug(f"Checking if company '{company_name}' already exists in the database.")

                # Check if company already exists
                cursor.execute(
                    "SELECT company_id FROM xbrl.company WHERE name = %s",
                    (company_name,)
                )
                result = cursor.fetchone()

                if result:
                    self.logger.info(f"Company '{company_name}' found in database. Inserting filing data.")
                    company_id = result['company_id']
                    self._insert_filing_data(cursor, company_id, extracted_data)
                    conn.commit()
                    self.logger.debug("Filing data committed to database.")
                    return company_id

                else:
                    self.logger.info(f"Company '{company_name}' not found. Initiating full processing.")
                    return self._process_new_company(conn, cursor, extracted_data)

        except Exception as e:
            self.logger.error(f"Error storing company metadata: {str(e)}", exc_info=True)
            if conn:
                self.logger.debug("Rolling back database transaction due to error.")
                conn.rollback()
            return None

        finally:
            if conn:
                self.logger.debug("Closing database connection.")
                conn.close()

    def _get_or_create_classification(self, cursor, source: str, level: str, name: str, code: str = None, parent_id: str = None):
        """Get or create industry classification and return its ID"""
        try:
            cursor.execute(
                """
                INSERT INTO xbrl.industry_classification (
                    source, level, name, code, parent_id
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (source, level, name) 
                DO UPDATE SET code = EXCLUDED.code
                RETURNING classification_id
                """,
                (source, level, name, code, parent_id))
            result = cursor.fetchone()
            return result['classification_id'] if result else None
        except Exception as e:
            self.logger.error(f"Failed to get/create classification: {str(e)}")
            return None

    def _process_industry_data(self, cursor, yahoo_results, extracted_data):
        """Process industry data and return primary classification ID"""
        primary_classification_id = None

        # Then handle GICS/Yahoo data
        if yahoo_results.get('sector'):
            self.logger.debug(f"Processing GICS sector: {yahoo_results['sector']}")
            
            sector_id = self._get_or_create_classification(
                cursor, 'GICS', 'SECTOR', 
                yahoo_results['sector']
            )
            
            if sector_id:
                self.logger.debug(f"GICS sector ID obtained: {sector_id}")
            else:
                self.logger.warning(f"Failed to get or create GICS sector for: {yahoo_results['sector']}")

            # First handle SIC if available (from SEC data)
            if 'sic' in extracted_data and sector_id:
                sic_description = extracted_data.get('sec_industry', 'Unknown')
                sic_code = extracted_data['sic']
                self.logger.debug(f"Processing SIC industry: {sic_description} (SIC: {sic_code})")

                sic_classification_id = self._get_or_create_classification(
                    cursor, 'SIC', 'INDUSTRY', 
                    sic_description, 
                    sic_code,
                    parent_id=sector_id
                )

                if sic_classification_id:
                    self.logger.debug(f"SIC classification ID obtained: {sic_classification_id}")
                    primary_classification_id = sic_classification_id
                else:
                    self.logger.warning(f"Failed to get or create SIC classification for SIC: {sic_code}")

            # Then process GICS industry if available
            if yahoo_results.get('industry'):
                self.logger.debug(f"Processing GICS industry: {yahoo_results['industry']}")

                industry_id = self._get_or_create_classification(
                    cursor, 'GICS', 'INDUSTRY', 
                    yahoo_results['industry'],
                    parent_id=sic_classification_id
                )

                if industry_id:
                    self.logger.debug(f"GICS industry ID obtained: {industry_id}")
                    if not primary_classification_id:
                        primary_classification_id = industry_id
                else:
                    self.logger.warning(f"Failed to get or create GICS industry: {yahoo_results['industry']}")

        else:
            self.logger.info("No GICS sector data found in Yahoo results.")

        return primary_classification_id


    def _process_new_company(self, conn, cursor, extracted_data):
        """Handle full processing for a new company with industry-first approach"""
        # Step 1: Get ticker symbol from SEC API
        self.logger.debug(f"Fetching ticker symbol for CIK: {extracted_data['cik']}")
        ticker_symbol = get_ticker_from_cik(extracted_data['cik'], self.logger)
        if not ticker_symbol:
            self.logger.warning(f"No ticker symbol found for CIK {extracted_data['cik']}")
            return None
        self.logger.debug(f"Retrieved ticker symbol: {ticker_symbol}")

        # Step 2: Get company data from Yahoo
        self.logger.debug(f"Querying Yahoo Finance for ticker: {ticker_symbol}")
        yahoo_results = query_yahoo(ticker_symbol, self.logger)
        if not yahoo_results:
            self.logger.warning(f"Failed to get Yahoo data for {ticker_symbol}")
            return None
        self.logger.debug(f"Received Yahoo data for {ticker_symbol}: {yahoo_results}")

        # Step 3: Process country code
        country_name = yahoo_results.get('country')
        if not country_name:
            self.logger.warning("No country found in Yahoo data")
            country_code = "XX"
        else:
            country_code = get_iso_code(country_name, self.logger) or 'XX'
        self.logger.debug(f"Resolved country code: {country_code}")

        # Step 4: Get LEI if available
        self.logger.debug(f"Looking up LEI for company: {extracted_data['company_name']}")
        lei = get_lei_by_name(extracted_data, self.logger)
        if lei:
            self.logger.debug(f"Found LEI: {lei}")
        else:
            self.logger.info(f"No LEI found for {extracted_data['company_name']}")

        # Step 5: Process industry classifications
        self.logger.debug("Processing industry classification data")
        primary_classification_id = self._process_industry_data(cursor, yahoo_results, extracted_data)
        if primary_classification_id:
            self.logger.debug(f"Primary classification ID: {primary_classification_id}")
        else:
            self.logger.info("No primary industry classification found")

        # Step 6: Insert company data
        try:
            self.logger.info(f"Inserting or updating company record for: {extracted_data['company_name']}")
            cursor.execute(
                """
                INSERT INTO xbrl.company (
                    lei, name, country, ticker_symbol, exchange_code,
                    primary_industry_classification_id, former_name, name_change_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (ticker_symbol, exchange_code)
                DO UPDATE SET former_name = EXCLUDED.former_name,
                name_change_date = EXCLUDED.name_change_date
                RETURNING company_id
                """,
                (
                    lei,
                    extracted_data['company_name'],
                    country_code,
                    ticker_symbol,
                    yahoo_results.get('exchange_code'),
                    primary_classification_id,
                    extracted_data.get('former_name'),
                    extracted_data.get('name_change_date')
                )
            )
            result = cursor.fetchone()
            company_id = result['company_id']
            self.logger.info(f"Company record saved. Company ID: {company_id}")

            # Step 7: Link all classifications to company
            if primary_classification_id:
                self.logger.debug(f"Linking classification {primary_classification_id} to company {company_id}")
                cursor.execute(
                    """
                    INSERT INTO xbrl.company_industry_classification (
                        company_id, classification_id
                    ) VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (company_id, primary_classification_id)
                )

            # Step 8: Insert filing data
            self.logger.debug(f"Inserting filing data for company ID: {company_id}")
            self._insert_filing_data(cursor, company_id, extracted_data)

            conn.commit()
            self.logger.debug("Database transaction committed successfully.")
            return company_id

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to insert company data: {str(e)}", exc_info=True)
            raise

    def _get_context_hash(self, context) -> str:
        if context is None:
            return None

        try:
            parts = []

            # Add period info
            if context.isStartEndPeriod:
                parts.append(f"duration:{context.startDatetime.date()}:{context.endDatetime.date()}")
            elif context.isInstantPeriod:
                parts.append(f"instant:{context.instantDatetime.date()}")
            else:
                parts.append("unknown")

            # Add dimensions (sorted for consistency)
            if context.hasSegment:
                dims = []
                for dim, mem in sorted(context.segDimValues.items(), key=lambda x: x[0].qname.localName):
                    dim_name = dim.qname.localName
                    mem_qname = str(getattr(mem, 'memberQname', getattr(mem, 'qname', 'Unknown')))
                    dims.append(f"{dim_name}={mem_qname}")
                parts.append("dims:" + "|".join(dims))

            # Include filing ID (to fully scope uniqueness)
            parts.append(f"filing:{self.filing_id}")

            hash_input = "::".join(parts)
            return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()

        except Exception as e:
            self.logger.warning(f"Failed to build context hash: {str(e)}")
            return None

            
    def _insert_filing_data(self, cursor, company_id, extracted_data):
        """Helper method to insert filing data"""
        if all(key in extracted_data for key in ['accession_number', 'filed_as_of_date', 'filing_type']):
            try:
                self.logger.debug(f"Attempting to insert filing for accession number: {extracted_data['accession_number']}")
                
                cursor.execute(
                    """
                    INSERT INTO xbrl.filing (
                        company_id, accession_number, filing_date, filing_type, period_end
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (accession_number) DO NOTHING
                    RETURNING filing_id
                    """,
                    (
                        company_id,
                        extracted_data['accession_number'],
                        extracted_data['filed_as_of_date'],
                        extracted_data['filing_type'],
                        extracted_data['period_end']
                    )
                )

                if cursor.rowcount > 0:
                    # New record was inserted
                    self.filing_id = cursor.fetchone()[0]
                    self.logger.info(f"Inserted new filing: {extracted_data['accession_number']} (filing_id: {self.filing_id})")
                else:
                    # Record already exists, fetch the existing ID
                    self.logger.info(f"Filing already exists for accession number: {extracted_data['accession_number']}. Fetching existing filing_id.")
                    cursor.execute(
                        "SELECT filing_id FROM xbrl.filing WHERE accession_number = %s",
                        (extracted_data['accession_number'],)
                    )
                    self.filing_id = cursor.fetchone()[0]
                    self.logger.debug(f"Existing filing_id retrieved: {self.filing_id}")

            except Exception as e:
                self.logger.error(f"Failed to insert or fetch filing data for accession number {extracted_data.get('accession_number')}: {str(e)}", exc_info=True)


    def _extract_submission_metadata(self, file_path: str):
        """Extract metadata from SEC submission text file"""

        self.logger.info(f"Extracting metadata from submission file: {file_path}")
        extracted_data = {}

        patterns = {
            "cik": re.compile(rb'CENTRAL INDEX KEY:\s+(\d+)'),
            "company_name": re.compile(rb'COMPANY CONFORMED NAME:\s+([^\n\r]+)'),
            "sic_full": re.compile(rb'STANDARD INDUSTRIAL CLASSIFICATION:\s+([^\n\r]+)'),
            "filing_type": re.compile(rb'CONFORMED SUBMISSION TYPE:\s+([^\n\r]+)'),
            "filed_as_of_date": re.compile(rb'FILED AS OF DATE:\s+(\d{8})'),
            "period_end": re.compile(rb'CONFORMED PERIOD OF REPORT:\s+(\d{8})'),
            "accession_number": re.compile(rb'ACCESSION NUMBER:\s+([^\n\r]+)'),
            "former_name": re.compile(rb'FORMER CONFORMED NAME:\s+([^\n\r]+)'),
            "name_change_date": re.compile(rb'DATE OF NAME CHANGE:\s+([^\n\r]+)'),
        }

        with open(file_path, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                for key, pattern in patterns.items():
                    match = pattern.search(mm)
                    if match:
                        value = match.group(1).decode("utf-8").strip()
                        extracted_data[key] = value
                        self.logger.debug(f"Extracted {key}: {value}")
                    else:
                        self.logger.debug(f"{key} not found in submission file")

        # Clean and parse SIC code
        if "sic_full" in extracted_data:
            sic_parts = re.match(r'(.+?)\s*\[(\d+)\]', extracted_data["sic_full"])
            if sic_parts:
                extracted_data["sic"] = sic_parts.group(2)
                extracted_data["sec_industry"] = sic_parts.group(1).strip().title()
                self.logger.debug(f"Parsed SIC code: {extracted_data['sic']}, Industry: {extracted_data['sec_industry']}")
            else:
                self.logger.warning(f"Unable to parse SIC from string: {extracted_data['sic_full']}")
            del extracted_data["sic_full"]

        # Format dates
        def format_date_field(field_name):
            try:
                date_str = extracted_data[field_name]
                extracted_data[field_name] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                self.logger.debug(f"Formatted {field_name}: {extracted_data[field_name]}")
            except Exception as e:
                self.logger.warning(f"Failed to format date for {field_name}: {str(e)}")

        for date_field in ["filed_as_of_date", "name_change_date", "period_end"]:
            if date_field in extracted_data:
                format_date_field(date_field)

        # Clean company name
        if "company_name" in extracted_data:
            original_name = extracted_data["company_name"]
            extracted_data["company_name"] = re.sub(r'[^a-zA-Z0-9\s]', '', original_name)
            extracted_data["company_name"] = re.sub(r'\s+', ' ', extracted_data["company_name"]).strip()
            self.logger.debug(f"Cleaned company name from '{original_name}' to '{extracted_data['company_name']}'")

        self.logger.info(f"Metadata extraction complete. Extracted keys: {list(extracted_data.keys())}")
        return extracted_data


    def load_xbrl_file(self, xbrl_file, validate=True, strict_validation=False):
        """Enhanced XBRL loading with troubleshooting for older filings"""
        try:
            # First try standard loading
            self.logger.info(f"Attempting to load XBRL file: {xbrl_file}")
            file_source = FileSource.openFileSource(xbrl_file, self.cntlr)
            
            reporting_year = int(extract_year(xbrl_file, self.logger))
            # Special handling for older filings
            load_options = {
                'disclosureSystem': 'efm-pragmatic' if reporting_year < 2018 
                                else 'efm',
                'validate': validate,
                'strictValidation': strict_validation,
                'cache': True, # Reuse downloaded taxonomies
                'loadDTS': True  # Ensure full taxonomy loading  
            }
            
            self.modelXbrl = self.cntlr.modelManager.load(
                file_source,
                **load_options
            )
                
            if self.modelXbrl is not None:  
                return True
            
        except Exception as e:
            self.logger.warning(f"Critical error loading {xbrl_file}: {str(e)}")
            return False

    def build_presentation_paths(self):
        arcrole = "http://www.xbrl.org/2003/arcrole/parent-child"
        rel_set = self.modelXbrl.relationshipSet(arcrole)
        concept_paths = {}

        def walk(concept, path_so_far):
            new_path = path_so_far + [concept.qname]
            path_str = " > ".join(str(qname) for qname in new_path)

            # Initialize list if this concept hasn't been seen
            if concept.qname not in concept_paths:
                concept_paths[concept.qname] = []

            # Append path if it's unique
            if path_str not in concept_paths[concept.qname]:
                concept_paths[concept.qname].append(path_str)

            # Recurse on children
            for rel in rel_set.fromModelObject(concept):
                child = rel.toModelObject
                if child is not None:
                    walk(child, new_path)

        for root_concept in rel_set.rootConcepts:
            walk(root_concept, [])

        return concept_paths


    def _process_standard_facts(self, company_id):
        batch_size = 1000  # Adjust based on your database performance

        self.logger.info("Starting XBRL fact extraction and processing...")

        # First collect all facts and their related concepts/contexts
        all_facts = []
        concepts_to_load = set()
        context_hashes = {}

        concept_paths = self.build_presentation_paths()
        self.logger.debug("Built presentation concept paths.")

        for fact in self.modelXbrl.facts:
            try:
                concepts_to_load.add(fact.concept)
                ctx_hash = self._get_context_hash(fact.context)
                if ctx_hash and ctx_hash not in context_hashes:
                    context_hashes[ctx_hash] = fact.context
                all_facts.append(fact)
            except Exception as e:
                concept_name = getattr(fact.concept.qname, 'localName', 'unknown') if hasattr(fact.concept, 'qname') else 'unknown'
                self.logger.warning(f"Error collecting fact {concept_name}: {str(e)}")
                continue

        self.logger.info(f"Collected {len(all_facts)} facts to process.")
        self.logger.debug(f"Unique concepts: {len(concepts_to_load)}, Unique contexts: {len(context_hashes)}")

        # Batch load all concepts and contexts first
        self.logger.debug("Loading concepts and contexts in batch...")
        concept_map = self.load_concepts_batch(concepts_to_load)  # Returns {(name,ns): concept_id}
        context_map = self.load_contexts_batch(context_hashes)  
        self.process_xbrl_relationships(concept_paths)
        self.logger.debug("Batch loading complete.")

        facts_to_insert = []

        self.logger.debug(f"Context map keys (hashes): {list(context_map.keys())[:5]}")
        for fact in all_facts:
            try:
                concept_key = (fact.concept.qname.localName, fact.concept.qname.namespaceURI)
                concept_id = concept_map.get(concept_key)
                context_id = context_map.get(self._get_context_hash(fact.context))

                if not context_id or not concept_id:
                    self.logger.debug(f"Skipping fact with missing concept/context ID: {concept_key}")
                    continue

                value_str = fact.value.strip() if fact.value else None

                target_column, parsed_value = self._parse_xbrl_value(
                    value_str,
                    getattr(fact.concept, 'typeQname', None).localName if hasattr(fact.concept, 'typeQname') else None
                )

                if target_column is None:
                    target_column = "string_value"
                    parsed_value = value_str

                unit = self._extract_unit(fact.unit) if hasattr(fact, 'unit') else "unitless"
                has_segment = fact.context.hasSegment
                has_scenario = fact.context.hasScenario
                decimals = getattr(fact, 'decimals', None)
                pres_path = concept_paths.get(fact.qname)

                facts_to_insert.append((
                    context_id,
                    concept_id,
                    parsed_value,
                    unit,
                    self.filing_id,
                    target_column,
                    has_segment,
                    has_scenario,
                    decimals,
                    company_id,
                    pres_path
                ))

            except Exception as e:
                concept_name = getattr(fact.concept.qname, 'localName', 'unknown') if hasattr(fact.concept, 'qname') else 'unknown'
                self.logger.warning(f"Error processing fact {concept_name}: {str(e)}", exc_info=True)
                continue

        self.logger.info(f"Prepared {len(facts_to_insert)} facts for insertion in batches of {batch_size}.")
        return self._insert_fact_batches(facts_to_insert, batch_size)


    def _insert_fact_batches(self, facts_to_insert, batch_size):
        fact_count = 0
        base_insert = """
            INSERT INTO xbrl.reported_fact (
                context_id, concept_id, %s, unit, filing_id, has_segment, has_scenario, decimals, company_id, presentation_path
            ) VALUES (%%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s, %%s)
        """

        self.logger.info(f"Starting batch insert of {len(facts_to_insert)} facts with batch size {batch_size}.")

        try:
            with self._get_db_connection() as conn:
                with conn.cursor() as cursor:
                    self.logger.debug(f"Deleting existing facts for filing_id={self.filing_id}")
                    cursor.execute("""
                        DELETE FROM xbrl.reported_fact
                        WHERE filing_id = %s
                    """, (self.filing_id,))
                    self.logger.info("Existing facts deleted.")

                    for i in range(0, len(facts_to_insert), batch_size):
                        batch = facts_to_insert[i:i + batch_size]
                        column_groups = {}

                        for fact in batch:
                            context_id, concept_id, parsed_value, unit, filing_id, target_column, \
                            has_segment, has_scenario, decimals, company_id, pres_path = fact

                            processed_decimals = None if decimals == 'INF' else decimals

                            if target_column not in column_groups:
                                column_groups[target_column] = []

                            column_groups[target_column].append((
                                context_id,
                                concept_id,
                                parsed_value,
                                unit,
                                filing_id,
                                has_segment,
                                has_scenario,
                                processed_decimals,
                                company_id,
                                pres_path
                            ))

                        for target_column, group in column_groups.items():
                            stmt = base_insert % target_column
                            self.logger.debug(f"Inserting {len(group)} facts into column: {target_column}")
                            psycopg2.extras.execute_batch(cursor, stmt, group)

                        fact_count += len(batch)
                        conn.commit()
                        self.logger.info(f"Inserted batch {i // batch_size + 1}: {len(batch)} facts committed.")

        except Exception as e:
            self.logger.error(f"Error during batch insert: {str(e)}", exc_info=True)
            raise

        self.logger.info(f"Completed insertion of {fact_count} facts.")
        return fact_count

    def _parse_xbrl_value(self, value_str, concept_type=None):
        """
        Parse XBRL string values into proper Python types.
        Returns: (target_column, parsed_value) or (None, None) if unparseable.
        """
        if value_str is None:
            return None, None

        try:
            
            # 1. Extract text if in html
            if ('<div' in value_str or '<table' in value_str or '<font' in value_str):
                return "string_value", get_cleaned_value(value_str, self.logger) 

            cleaned = value_str.strip()

            # 1. Boolean detection (case-insensitive)
            if cleaned.lower() in ('true', 'false', 'yes', 'no'):
                return "boolean_value", cleaned.lower() in ('true', 'yes')

            # 2. Date detection (uses concept type hint)
            if concept_type and 'date' in concept_type.lower():
                try:
                    return "date_value", parser.parse(cleaned).date()
                except:
                    pass

            # 3. Numeric handling (aggressive cleaning)
            numeric_cleaned = (
                cleaned.replace(',', '')
                .replace('−', '-')
                .replace('(', '-').replace(')', '')
                .replace('$', '').replace('%', '')
                .strip()
            ) 

            # Special numeric tokens (INF, NaN)
            if numeric_cleaned.upper() in ('INF', '+INF'):
                return "numeric_value", float('inf')
            if numeric_cleaned.upper() == '-INF':
                return "numeric_value", float('-inf')
            if numeric_cleaned.upper() in ('NAN', 'NA'):
                return "numeric_value", float('nan')

            # Attempt numeric conversion
            if numeric_cleaned.replace('.', '', 1).lstrip('-').isdigit():
                try:
                    return "numeric_value", float(numeric_cleaned)
                except ValueError:
                    pass

            # Fallback to string if no other type matches
            return "string_value", value_str

        except Exception:
            return None, None

    def _extract_unit(self, unit):
        """Extract unit name from XBRL unit object"""
        try:
            if unit and unit.measures and unit.measures[0]:
                return str(unit.measures[0][0].localName)
        except Exception:
            pass
        return "unitless"


    def load_contexts_batch(self, contexts):
        """Batch load multiple contexts at once"""
        if not contexts:
            self.logger.info("No contexts provided to load_contexts_batch.")
            return {}

        conn = None
        context_map = {}  # Maps context objects to their context_id
        self.logger.info(f"Starting batch context load for {len(contexts)} contexts.")

        try:
            conn = self._get_db_connection()
            with conn.cursor() as cursor:
                period_contexts = []
                instant_contexts = []
                dimension_names = set()
                dimension_members = set()
                context_dimensions = []

                for ctx_hash, context in contexts.items():
                    if context is None:
                        continue

                    period_start = period_end = instant = None
                    try:
                        if context.isStartEndPeriod:
                            period_start = context.startDatetime.date()
                            period_end = context.endDatetime.date()
                            period_contexts.append((self.filing_id, period_start, period_end, ctx_hash))
                        elif context.isInstantPeriod:
                            instant = context.instantDatetime.date()
                            instant_contexts.append((self.filing_id, instant, ctx_hash))
                    except AttributeError:
                        self.logger.warning("Missing date info in context object.")
                        continue

                    try:
                        if context.hasSegment:
                            for dim, mem in context.segDimValues.items():
                                try:
                                    dim_name = dim.qname.localName
                                    mem_qname = str(getattr(mem, 'memberQname', getattr(mem, 'qname', 'Unknown')))
                                    mem_name = getattr(mem, 'memberQname', getattr(mem, 'qname', 'Unknown')).localName
                                    mem_ns = getattr(mem, 'memberQname', getattr(mem, 'qname', 'Unknown')).namespaceURI
                                    
                                    # Immediately record each (context, dimension, member)
                                    context_dimensions.append((ctx_hash, dim_name, mem_qname, mem_name, mem_ns))
                                    
                                    dimension_names.add(dim_name)
                                    dimension_members.add((dim_name, mem_qname, mem_name, mem_ns))
                                except Exception as e:
                                    self.logger.warning(f"Error parsing dimension in context: {str(e)}")
                                    continue
                    except AttributeError:
                        pass


                self.logger.debug(f"Found {len(period_contexts)} period contexts, {len(instant_contexts)} instant contexts.")
                self.logger.debug(f"Found {len(dimension_names)} unique dimension names and {len(dimension_members)} member pairs.")

                # --- 1. Insert periods ---
                for filing_id, start, end, ctx_hash in period_contexts:
                    cursor.execute("""
                        INSERT INTO xbrl.context_period (period_type, period_start, period_end, instant_date, filing_id)
                        VALUES ('duration', %s, %s, NULL, %s)
                        ON CONFLICT (period_start, period_end, filing_id)
                        WHERE (period_start IS NOT NULL AND period_end IS NOT NULL)
                        DO UPDATE SET period_end = EXCLUDED.period_end
                        RETURNING context_period_id
                    """, (start, end, filing_id))
                    period_id = cursor.fetchone()
                    if not period_id:
                        cursor.execute("SELECT context_period_id FROM xbrl.context_period WHERE period_start = %s AND period_end = %s", (start, end))
                        period_id = cursor.fetchone()
                    cursor.execute("""
                        INSERT INTO xbrl.context (filing_id, period_id)
                        VALUES (%s, %s)
                        RETURNING context_id
                    """, (self.filing_id, period_id[0]))
                    context_map[ctx_hash] = cursor.fetchone()[0]

                for filing_id, instant, ctx_hash in instant_contexts:
                    cursor.execute("""
                        INSERT INTO xbrl.context_period (filing_id, period_start, period_end, instant_date, period_type)
                        VALUES (%s, NULL, NULL, %s, 'instant')
                        ON CONFLICT (filing_id, instant_date)
                        WHERE (instant_date IS NOT NULL)
                        DO UPDATE SET instant_date = EXCLUDED.instant_date
                        RETURNING context_period_id
                    """, (filing_id, instant))
                    period_id = cursor.fetchone()
                    if not period_id:
                        cursor.execute("SELECT context_period_id FROM xbrl.context_period WHERE instant_date = %s", (instant,))
                        period_id = cursor.fetchone()
                    cursor.execute("""
                        INSERT INTO xbrl.context (filing_id, period_id)
                        VALUES (%s, %s)
                        RETURNING context_id
                    """, (self.filing_id, period_id[0]))
                    context_map[ctx_hash] = cursor.fetchone()[0]

                # --- 2. Dimensions and Members ---
                if dimension_names:
                    self.logger.info(f"Inserting {len(dimension_names)} dimension declarations.")
                    psycopg2.extras.execute_batch(cursor,
                        """
                        INSERT INTO xbrl.dimension_declaration (dimension_name, filing_id)
                        VALUES (%s, %s)
                        
                        """,
                        # --ON CONFLICT (dimension_name, filing_id) DO NOTHING
                        [(d, self.filing_id) for d in dimension_names]
                    )
                    cursor.execute("SELECT dimension_id, dimension_name FROM xbrl.dimension_declaration WHERE dimension_name IN %s", (tuple(dimension_names),))
                    dimension_ids = {row[1]: row[0] for row in cursor.fetchall()}
                else:
                    dimension_ids = {}

                if dimension_members:
                    self.logger.info(f"Inserting {len(dimension_members)} dimension members.")
                    psycopg2.extras.execute_batch(cursor,
                        """
                        INSERT INTO xbrl.dimension_member (dimension_id, member_qname, member_name, member_ns, filing_id)
                        VALUES (%s, %s, %s, %s, %s)
                        
                        """,
                        # -- ON CONFLICT (dimension_id, member_qname, filing_id) DO NOTHING
                        [(dimension_ids[d], mq, m, m_ns, self.filing_id) for d, mq, m, m_ns in dimension_members]
                    )
                    cursor.execute("""
                        SELECT dm.member_id, dm.member_qname, dm.member_name, dm.member_ns, dm.dimension_id
                        FROM xbrl.dimension_member dm
                        WHERE (dm.dimension_id, dm.member_qname, dm.member_name, dm.member_ns) IN %s
                    """, (tuple((dimension_ids[d], qm, m, m_ns) for d, qm, m, m_ns in dimension_members),))
                    member_ids = {(row[4],row[3], row[2]): row[0] for row in cursor.fetchall()}
                else:
                    member_ids = {}

                # --- 3. Link Context to Dimension+Member ---
                context_dim_member_rows = []
                for ctx_hash, dim_name, mem_qname, mem_name, mem_ns in context_dimensions:
                    ctx_id = context_map.get(ctx_hash)
                    dim_id = dimension_ids.get(dim_name)
                    mem_id = member_ids.get((dim_id, mem_ns, mem_name))
                    if ctx_id and dim_id and mem_id:
                        context_dim_member_rows.append((ctx_id, dim_id, mem_id, self.filing_id))

                if context_dim_member_rows:
                    self.logger.info(f"Linking {len(context_dim_member_rows)} context-dimension-member records.")
                    psycopg2.extras.execute_batch(cursor,
                        """
                        INSERT INTO xbrl.context_dimension_members (context_id, dimension_id, member_id, filing_id)
                        VALUES (%s, %s, %s, %s)
                        
                        """,
                        # -- ON CONFLICT (context_id, dimension_id, filing_id) DO NOTHING
                        context_dim_member_rows
                    )

                conn.commit()
                self.logger.info(f"Context loading complete. Loaded {len(context_map)} context records.")
                return context_map

        except Exception as e:
            self.logger.error(f"Error in load_contexts_batch: {str(e)}", exc_info=True)
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

            
    def load_concepts_batch(self, concepts):
        """Batch load multiple concepts at once"""
        if not concepts:
            self.logger.debug("No concepts provided to load_concepts_batch, returning empty dict")
            return {}

        conn = None
        concept_map = {}  # Maps (concept_name, namespace) to concept_id
        try:
            self.logger.info(f"Starting batch load of {len(concepts)} concepts")
            conn = self._get_db_connection()
            with conn.cursor() as cursor:
                # --- 1. Prepare batch data ---
                concept_data = []
                concept_attr_data = []
                label_data = []
                verbose_labels = []
                docs_data = []
                
                valid_concepts = 0
                skipped_concepts = 0
                
                for concept in concepts:
                    if concept is None:
                        skipped_concepts += 1
                        continue
                    
                    concept_name = concept.qname.localName if hasattr(concept, 'qname') else None
                    namespace = concept.qname.namespaceURI if hasattr(concept, 'qname') else None
                    std_label = concept.label() if hasattr(concept, 'label') else None
                    concept_qname = str(concept.qname) if hasattr(concept, 'qname') else None
                    
                    if not concept_name or not namespace:
                        skipped_concepts += 1
                        self.logger.debug(f"Skipping concept - missing name or namespace: {concept}")
                        continue
                    
                    valid_concepts += 1
                    # Store for concept table
                    concept_data.append((concept_name, namespace, self.filing_id, concept_qname))
                    
                    # Store for attribute table (we'll get concept_id later)
                    concept_attr_data.append((
                        concept_name, namespace,
                        getattr(concept, 'periodType', None),
                        str(getattr(concept, 'typeQname', None)),
                        getattr(concept, 'balance', None),
                        self.filing_id
                    ))
                    
                    # Store for label table
                    label_data.append((concept_name, namespace, std_label, self.filing_id))
                    
                    # Store verbose labels if available
                    if hasattr(concept, 'label'):
                        verbose_label = concept.label('http://www.xbrl.org/2003/role/verboseLabel')
                        if verbose_label:
                            verbose_labels.append((concept_name, namespace, verbose_label, self.filing_id))
                    
                    # Store documentation if available
                    if hasattr(concept, 'genDocs'):
                        docs = [doc.value.strip() for doc in concept.genDocs 
                            if hasattr(doc, 'value') and doc.value]
                        if docs:
                            docs_data.append((concept_name, namespace, '\n\n'.join(docs), self.filing_id))
                
                self.logger.info(f"Processed {valid_concepts} valid concepts, skipped {skipped_concepts}")
                
                # --- 2. Batch insert concepts ---
                if concept_data:
                    self.logger.debug(f"Inserting {len(concept_data)} concepts into xbrl.concept")
                    psycopg2.extras.execute_batch(cursor,
                        """
                        INSERT INTO xbrl.concept (concept_name, namespace, filing_id, concept_qname)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (concept_name, namespace, filing_id) DO NOTHING
                        """,
                        concept_data
                    )
                    
                    # Get mapping of (concept_name, namespace) to concept_id
                    cursor.execute(
                        """
                        SELECT concept_name, namespace, concept_id FROM xbrl.concept
                        WHERE (concept_name, namespace) IN %s
                        """,
                        (tuple((c[0], c[1]) for c in concept_data),)
                    )
                    
                    for row in cursor:
                        concept_map[(row[0], row[1])] = row[2]
                    
                    self.logger.debug(f"Mapped {len(concept_map)} concepts to their IDs")
                
                # --- 3. Batch insert concept attributes ---
                if concept_attr_data and concept_map:
                    attr_data_with_ids = []
                    for data in concept_attr_data:
                        key = (data[0], data[1])
                        if key in concept_map:
                            attr_data_with_ids.append((
                                concept_map[key], data[2], data[3], data[4], self.filing_id
                            ))
                    
                    self.logger.debug(f"Inserting {len(attr_data_with_ids)} concept attributes")
                    psycopg2.extras.execute_batch(cursor,
                        """
                        INSERT INTO xbrl.concept_attribute (
                            concept_id, period_type, data_type, balance_type,
                            filing_id
                        ) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (concept_id) DO UPDATE SET
                            period_type = EXCLUDED.period_type,
                            data_type = EXCLUDED.data_type,
                            balance_type = EXCLUDED.balance_type
                        """,
                        attr_data_with_ids
                    )
                
                # --- 4. Batch insert labels ---
                if label_data and concept_map:
                    label_data_with_ids = []
                    for data in label_data:
                        key = (data[0], data[1])
                        if key in concept_map:
                            label_data_with_ids.append((concept_map[key], data[2], self.filing_id))
                    
                    self.logger.debug(f"Inserting {len(label_data_with_ids)} standard labels")
                    psycopg2.extras.execute_batch(cursor,
                        """
                        INSERT INTO xbrl.label (
                            concept_id, standard_label, filing_id
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT (concept_id) DO UPDATE SET
                            standard_label = EXCLUDED.standard_label
                        """,
                        label_data_with_ids
                    )
                
                # --- 5. Batch update verbose labels ---
                if verbose_labels and concept_map:
                    self.logger.debug(f"Updating {len(verbose_labels)} verbose labels")
                    for data in verbose_labels:
                        key = (data[0], data[1])
                        if key in concept_map:
                            cursor.execute(
                                """
                                UPDATE xbrl.label
                                SET verbose_label = %s
                                WHERE concept_id = %s
                                """,
                                (data[2], concept_map[key])
                            )
                
                # --- 6. Batch update documentation ---
                if docs_data and concept_map:
                    self.logger.debug(f"Updating {len(docs_data)} documentation entries")
                    for data in docs_data:
                        key = (data[0], data[1])
                        if key in concept_map:
                            cursor.execute(
                                """
                                UPDATE xbrl.label
                                SET documentation = %s
                                WHERE concept_id = %s
                                """,
                                (data[2], concept_map[key])
                            )
                
                conn.commit()
                self.logger.info(f"Successfully loaded {len(concept_map)} concepts")
                return concept_map
                
        except Exception as e:
            self.logger.error(f"Error in load_concepts_batch: {str(e)}", exc_info=True)
            if conn: conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
                self.logger.debug("Database connection closed")
    
    def process_xbrl_relationships(self, concept_paths):
        self.logger.info("Starting relationship processing")
        role_data = set()

        try:
            with self._get_db_connection() as conn:
                with conn.cursor() as cursor:
                    self.logger.debug("Processing arcroles")
                    # Process arcroles with batch upsert
                    arcrole_data = [(uri,self.filing_id) for uri in self.modelXbrl.arcroleTypes]
                    arcrole_data.append(('http://www.xbrl.org/2003/arcrole/parent-child',self.filing_id))
                    
                    if arcrole_data:
                        self.logger.info(f"Preparing to upsert {len(arcrole_data)} arcroles")
                        args_str = ','.join(cursor.mogrify("(%s, %s)", x).decode('utf-8') for x in arcrole_data)
                        cursor.execute(f"""
                            INSERT INTO xbrl.arcrole (arcrole_uri, filing_id)
                            VALUES {args_str}
                            ON CONFLICT (arcrole_uri) 
                            DO UPDATE SET arcrole_uri = EXCLUDED.arcrole_uri
                        """)
                        self.logger.info(f"Successfully upserted {len(arcrole_data)} arcroles")

                    arcrole_to_table = {
                        # "hypercube-dimension": "xbrl.hypercube_dimension",
                        # "dimension-domain": "xbrl.dimension_domain",
                        # "domain-member": "xbrl.domain_member",
                        # "all": "xbrl.all_data",
                        # "notAll": "xbrl.notAll_data",
                        # "dimension-default": "xbrl.Dimension_Default",
                        # "fact-explanatoryFact": "xbrl.Explanatory_Fact",
                        # "summation-item": "xbrl.calculation_relationship",
                        "parent-child": "xbrl.concept_relationship"
                    }
                    self.logger.debug(f"Configured arcrole to table mapping with {len(arcrole_to_table)} entries")

                    # Preload arcroles
                    cursor.execute("SELECT arcrole_id, arcrole_uri FROM xbrl.arcrole")
                    arcrole_map = {row[1]: row[0] for row in cursor.fetchall()}
                    self.logger.info(f"Loaded {len(arcrole_map)} arcroles from database")

            self.logger.debug("Processing relationships for each arcrole")
            for arcrole_uri, arcrole_id in arcrole_map.items():
                rel_set = self.modelXbrl.relationshipSet(arcrole_uri)
                if rel_set is None:
                    self.logger.debug(f"No relationship set found for arcrole: {arcrole_uri}")
                    continue

                for rel in rel_set.modelRelationships:
                    # Collect role data
                    if hasattr(rel, 'linkrole') and rel.linkrole:
                        role_data.add((rel.linkrole,self.filing_id))

            self.logger.info(f"Collected {len(role_data)} unique roles from relationships")

            # Batch process roles
            if role_data:
                self.logger.info(f"Processing {len(role_data)} roles")
                with self._get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        args_str = ','.join(cursor.mogrify("(%s, %s)", x).decode('utf-8') for x in role_data)
                        cursor.execute(f"""
                            INSERT INTO xbrl.link_role (role_uri, filing_id)
                            VALUES {args_str}
                            ON CONFLICT (role_uri) 
                            DO UPDATE SET role_uri = EXCLUDED.role_uri
                            RETURNING role_uri, role_id
                        """)
                        role_map = {uri: rid for uri, rid in cursor.fetchall()}
                        conn.commit()
                        self.logger.info(f"Successfully processed {len(role_map)} roles")

            # Group rows by table
            data_by_table = defaultdict(list)
            relationship_count = 0
            skipped_relationships = 0

            self.logger.debug("Processing relationships and grouping by table")
            for arcrole_uri, arcrole_id in arcrole_map.items():
                rel_set = self.modelXbrl.relationshipSet(arcrole_uri)
                if rel_set is None:
                    continue

                for rel in rel_set.modelRelationships:
                    # Collect role data
                    if hasattr(rel, 'linkrole') and rel.linkrole:
                        role_data.add((rel.linkrole,))

                    if rel is None:
                        skipped_relationships += 1
                        continue
                    
                    try:
                        parent_qname = rel.fromModelObject.qname
                        child_qname = rel.toModelObject.qname
                        pres_path = concept_paths.get(child_qname)

                        parent_name = parent_qname.localName
                        parent_ns = parent_qname.namespaceURI
                        child_name = child_qname.localName
                        child_ns = child_qname.namespaceURI

                        for arcrole_fragment, table_name in arcrole_to_table.items():
                            if arcrole_fragment in arcrole_uri:
                                data_by_table[table_name].append((
                                    parent_name,
                                    parent_ns,
                                    str(parent_qname),
                                    child_name,
                                    child_ns,
                                    str(child_qname),
                                    arcrole_id,
                                    role_map.get(rel.linkrole) if hasattr(rel, 'linkrole') and rel.linkrole else None,
                                    self.filing_id,
                                    pres_path
                                ))
                                relationship_count += 1
                                break

                    except Exception as e:
                        skipped_relationships += 1
                        self.logger.warning(f"Error processing relationship: {e}", exc_info=True)
                        continue

            self.logger.info(f"Processed {relationship_count} relationships, skipped {skipped_relationships}")

            if not data_by_table:
                self.logger.warning("No relationship data found to insert")
                return

            self.logger.info(f"Preparing to insert relationships into {len(data_by_table)} tables")
            with self._get_db_connection() as conn:
                with conn.cursor() as cursor:
                    for table, rows in data_by_table.items():
                        if not rows:
                            self.logger.debug(f"Skipping empty insert for table {table}")
                            continue

                        self.logger.info(f"Inserting {len(rows)} relationships into {table}")
                        insert_query = f"""
                            INSERT INTO {table} (
                                parent_name, parent_ns, parent_qname, child_name, child_ns, child_qname, arcrole_id, role_id, filing_id, presentation_path
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """
                        execute_batch(cursor, insert_query, rows)
                        self.logger.info(f"Successfully inserted {len(rows)} rows into {table}")
                    
                    conn.commit()
                    self.logger.info("All relationship data committed successfully")

        except Exception as e:
            self.logger.error(f"Error in relationship processing: {str(e)}", exc_info=True)
            raise
        finally:
            self.logger.info("Completed relationship processing")

    def close(self):
        """Clean up resources"""
        self.logger.info("Starting resource cleanup")

        try:
            if self.modelXbrl:
                self.logger.debug("Closing modelXbrl through controller")
                try:
                    self.cntlr.modelManager.close()
                    self.logger.debug("Successfully closed modelXbrl")
                except Exception as e:
                    self.logger.error(f"Error closing modelXbrl: {str(e)}", exc_info=True)
                    raise
            else:
                self.logger.debug("No modelXbrl instance to clean up")

            if self.conn and not self.conn.closed:
                self.logger.debug("Closing database connection")
                try:
                    self.conn.close()
                    self.logger.debug("Successfully closed database connection")
                except Exception as e:
                    self.logger.error(f"Error closing database connection: {str(e)}", exc_info=True)
                    raise
            else:
                self.logger.debug("No active database connection to close")

            # Nullify references
            self.logger.debug("Nullifying instance references")
            self.modelXbrl = None
            self.fact_data = []
            self.conn = None

            self.logger.info("Resource cleanup completed successfully")

        except Exception as e:
            self.logger.error(f"Error during resource cleanup: {str(e)}", exc_info=True)
            raise

    def __del__(self):
        """Destructor to ensure resources are cleaned up"""
        self.close()

# Example usage:
if __name__ == "__main__":
    # Configure your database connection
    db_config = {
        'dbname': 'finhub',
        'user': 'finhub_admin',
        'password': 'pass@123',
        'host': 'localhost',
        'port': '5432'
    }

    loader = XBRLToPostgresLoader(db_config=db_config)


    try:
        # Load an XBRL file
        submission_file = r"C:\Users\mithu\Documents\MEGA\Projects\Financial_Data_Analytics_Pipeline\filings\0000831259\10-K\2023-12-31\0000831259-24-000011.txt"
        instance_file = r"C:\Users\mithu\Documents\MEGA\Projects\Financial_Data_Analytics_Pipeline\filings\0000831259\10-K\2023-12-31\fcx-20231231_htm.xml"
        
        if loader.load_xbrl_file(instance_file):
            # Store company metadata and get company ID
            company_id = loader.load_metadata(submission_file)
            
            # Process facts with the company ID
            fact_count = loader._process_standard_facts(company_id=company_id)
            print(f"Processed {fact_count} facts")
            
        else:
            print("Failed to load XBRL file")
    finally:
        loader.close()