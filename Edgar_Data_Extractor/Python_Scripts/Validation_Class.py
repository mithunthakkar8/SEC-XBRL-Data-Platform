import mmap
from arelle import Cntlr, FileSource
from collections import defaultdict
from urllib.parse import urljoin
import logging
import re
import psycopg2
from psycopg2.extras import Json
import os
import sys

class XBRLProcessor:
    def __init__(self, log_file='xbrl_processor.log', db_config=None):
        """Initialize the XBRL processor with enhanced error handling and PostgreSQL support"""
        self.cntlr = Cntlr.Cntlr(logFileName=log_file)
        self.modelXbrl = None
        self.fact_data = []
        self.grouped_facts = defaultdict(list)
        self.validation_errors = []

        
        # Initialize logger
        self.logger = logging.getLogger("XBRLProcessor")
        self.logger.setLevel(logging.DEBUG)  # Set the logging level

        # FileHandler for logging to a file
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        # StreamHandler for logging to console (stdout)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))

        # Add both handlers to the logger
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
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database connection and create tables if they don't exist"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = True
            cursor = self.conn.cursor()
            
            # Create company metadata table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS company_metadata (
                    id SERIAL PRIMARY KEY,
                    form_type VARCHAR(50),
                    filed_as_of_date DATE,
                    industry VARCHAR(255),
                    sic VARCHAR(10),
                    accession_number VARCHAR(50),
                    company_name VARCHAR(255),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create facts table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS xbrl_facts (
                    id SERIAL PRIMARY KEY,
                    company_id INTEGER REFERENCES company_metadata(id),
                    concept VARCHAR(255),
                    value TEXT,
                    unit VARCHAR(50),
                    context JSONB,
                    concept_details JSONB,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create validation errors table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS validation_errors (
                    id SERIAL PRIMARY KEY,
                    company_id INTEGER REFERENCES company_metadata(id),
                    error_code VARCHAR(50),
                    error_message TEXT,
                    file_path VARCHAR(255),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.logger.info("Database tables initialized successfully")
        except Exception as e:
            self.logger.error(f"Error initializing database: {str(e)}")
            if self.conn:
                self.conn.rollback()
            raise
    
    def _extract_year(self, xbrl_file_path):
        """Extract year from XSD filename (handles both hyphenated and compact dates)"""
        try:
            
            filename = os.path.basename(xbrl_file_path)
            year_match = re.search(r'''
                (?:^|[-_])              # Start or separator (- or _)
                (20\d{2})                # Year capture group
                (?:\d{4}|-\d{2}-\d{2})   # Either YYYYMMDD or -MM-DD
                \.xml$                   # Extension
            ''', filename, re.VERBOSE)
            
            if year_match:
                year = int(year_match.group(1))
                return year
                    
            raise ValueError(f"No valid year found in filename: {xbrl_file_path}")
            
        except Exception as e:
            self.logger.error(f"Year extraction failed: {str(e)}")
            return None


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
    
    def extract_company_metadata(self, submission_file_path):
        extracted_data = {}

        patterns = {
            "form_type": re.compile(rb'CONFORMED SUBMISSION TYPE:\s+([^\n\r]+)'),
            "filed_as_of_date": re.compile(rb'FILED AS OF DATE:\s+(\d{8})'),
            "sic_full": re.compile(rb'STANDARD INDUSTRIAL CLASSIFICATION:\s+([^\n\r]+)'),
            "accession_number": re.compile(rb'ACCESSION NUMBER:\s+([^\n\r]+)'),
            "company_name": re.compile(rb'COMPANY CONFORMED NAME:\s+([^\n\r]+)')
        }

        with open(submission_file_path, "rb") as f:  # Open in binary mode
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                for key, pattern in patterns.items():
                    match = pattern.search(mm)
                    if match:
                        extracted_data[key] = match.group(1).decode("utf-8").strip()

        # Convert filed_as_of_date from YYYYMMDD to YYYY-MM-DD
        if "filed_as_of_date" in extracted_data:
            date_str = extracted_data["filed_as_of_date"]
            extracted_data["filed_as_of_date"] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

        # Extract and split SIC information
        if "sic_full" in extracted_data:
            sic_parts = re.match(r'(.+?)\s*\[(\d+)\]', extracted_data["sic_full"])
            if sic_parts:
                extracted_data["industry"] = sic_parts.group(1).strip().title()
                extracted_data["sic"] = sic_parts.group(2)
            del extracted_data["sic_full"]  # Remove original sic_full key

        # Clean company name (remove special characters & extra spaces)
        if "company_name" in extracted_data:
            extracted_data["company_name"] = re.sub(r'[^a-zA-Z0-9\s]', '', extracted_data["company_name"])
            extracted_data["company_name"] = re.sub(r'\s+', ' ', extracted_data["company_name"]).strip()

        
        return extracted_data

    def store_company_metadata(self, xbrl_file):
        """Store company metadata in PostgreSQL database"""
        data = self.extract_company_metadata(xbrl_file)
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Check if this record already exists
            cursor.execute("""
                SELECT id FROM company_metadata 
                WHERE accession_number = %s
            """, (data['accession_number'],))
            
            existing_record = cursor.fetchone()
            
            if existing_record:
                company_id = existing_record[0]
                self.logger.info(f"Using existing company record with ID: {company_id}")
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO company_metadata (
                        form_type, filed_as_of_date, industry, sic, 
                        accession_number, company_name
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    data['form_type'], data['filed_as_of_date'], data['industry'],
                    data['sic'], data['accession_number'], data['company_name']
                ))
                company_id = cursor.fetchone()[0]
                self.logger.info(f"Inserted new company record with ID: {company_id}")
            
            conn.commit()
            return company_id
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error storing company metadata: {str(e)}")
            raise

    def _handle_validation_error(self, error_code, error_message, file_path):
        """Handle and log validation errors"""
        error_info = {
            'code': error_code,
            'message': error_message,
            'file': file_path
        }
        self.validation_errors.append(error_info)
        self.logger.warning(f"Validation Error [{error_code}]: {error_message} in {file_path}")
        
        if "XML declaration allowed only at the start" in error_message:
            self.logger.info("This often occurs when there's embedded XML in document text")
        elif "Entity 'nbsp' not defined" in error_message:
            self.logger.info("This typically indicates HTML entities in the document")

    def load_xbrl_file(self, xbrl_file, validate=True, strict_validation=False):
        """Enhanced XBRL loading with troubleshooting for older filings"""
        try:
            # First try standard loading
            self.logger.info(f"Attempting to load XBRL file: {xbrl_file}")
            file_source = FileSource.openFileSource(xbrl_file, self.cntlr)
            
            reporting_year = int(self._extract_year(xbrl_file))
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
                **load_options,
                errorHandler=self._handle_validation_error
            )
                
            if self.modelXbrl is not None:  
                return True
            
        except Exception as e:
            self.logger.warning(f"Critical error loading {xbrl_file}: {str(e)}")
            return False

    def _is_complete_xbrl(self, content):
        """Check if the content appears to be a complete XBRL document"""
        if not content:
            return False
        # Basic check for opening and closing tags
        has_opening = ('<xbrl' in content.lower())
        has_closing = ('</xbrl>' in content.lower())
        return has_opening and has_closing


    def process_facts(self, skip_non_numeric=True, company_id=None):
        if not self.modelXbrl:
            self.logger.error("No XBRL model loaded - cannot process facts")
            return 0
            
        self.fact_data = []

        try:
            self.logger.info(f"Attempting Standard XBRL processing")
            fact_count = self._process_standard_facts(skip_non_numeric, company_id)
            
            if fact_count > 0:
                self.logger.info(f"Standard XBRL processing found {fact_count} facts")
                return fact_count
                
        except Exception as e:
            self.logger.error(f"Standard XBRL processing failed: {str(e)}")

    def save_to_database(self, company_id):
        """Save processed data to PostgreSQL with better error handling"""
        if not self.fact_data:
            self.logger.warning("No facts to save, but storing metadata anyway")
            
            # Store at least the validation errors if any
            if self.validation_errors:
                try:
                    conn = self._get_db_connection()
                    cursor = conn.cursor()
                    for error in self.validation_errors:
                        cursor.execute("""
                            INSERT INTO validation_errors (
                                company_id, error_code, error_message, file_path
                            ) VALUES (%s, %s, %s, %s)
                        """, (
                            company_id,
                            error['code'],
                            error['message'],
                            error['file']
                        ))
                    conn.commit()
                    return True
                except Exception as e:
                    conn.rollback()
                    self.logger.error(f"Error saving validation errors: {str(e)}")
                    return False
            return False
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Insert facts in batches
            batch_size = 100
            for i in range(0, len(self.fact_data), batch_size):
                batch = self.fact_data[i:i + batch_size]
                args = [(fact['company_id'], 
                        fact['concept'], 
                        fact['value'], 
                        fact['unit'], 
                        Json(fact['context']), 
                        Json(fact['concept_details'])) for fact in batch]
                
                cursor.executemany("""
                    INSERT INTO xbrl_facts (
                        company_id, concept, value, unit, context, concept_details
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, args)
            
            # Insert validation errors if any
            for error in self.validation_errors:
                cursor.execute("""
                    INSERT INTO validation_errors (
                        company_id, error_code, error_message, file_path
                    ) VALUES (%s, %s, %s, %s)
                """, (
                    company_id,
                    error['code'],
                    error['message'],
                    error['file']
                ))
            
            conn.commit()
            self.logger.info(f"Successfully saved {len(self.fact_data)} facts to database")
            return True
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error saving to database: {str(e)}")
            raise

    def _process_standard_facts(self, skip_non_numeric, company_id):
        """Process standard XBRL facts"""
        fact_count = 0
        for fact in self.modelXbrl.facts:
            try:
                if fact.concept is None:
                    continue
                    
                concept_details = self._get_concept_details(fact.concept)
                
                try:
                    fact_value = fact.value
                except (ValueError, TypeError):
                    if skip_non_numeric:
                        continue
                
                context_details = self._get_context_details(fact.context)
                
                unit = "unitless"
                try:
                    if fact.unit and fact.unit.measures:
                        unit = str(fact.unit.measures[0][0].localName)
                except Exception:
                    pass
                
                fact_entry = {
                    'company_id': company_id,
                    'concept': concept_details.get('name'),
                    'value': fact_value,
                    'unit': unit,
                    'context': context_details,
                    'concept_details': {k: v for k, v in concept_details.items() if k != 'name'}
                }
                
                self.fact_data.append(fact_entry)
                fact_count += 1
                
            except Exception as e:
                self.logger.warning(f"Error processing fact: {str(e)}")
                continue
                
        return fact_count


    def _get_context_details(self, context):
        """Extract context details with error handling"""
        try:
            details = {}
            
            if context is not None:
                try:
                    if context.startDatetime and context.endDatetime:
                        details['period'] = {
                            'start': str(context.startDatetime.date()),
                            'end': str(context.endDatetime.date())
                        }
                    elif context.instantDatetime:
                        details['instant'] = str(context.instantDatetime.date())
                    else:
                        details['period'] = 'unknown'
                except AttributeError:
                    details['period'] = 'error'
                    self.logger.warning("Failed to extract period information")
                
                try:
                    if context.entityIdentifier:
                        details['entity'] = context.entityIdentifier[1]
                except (AttributeError, IndexError):
                    pass
                
                try:
                    if context.scenario is not None:
                        scenario_qnames = [qname.localName for qname in context.scenario.qnameIter()]
                        if scenario_qnames:
                            details['scenario'] = scenario_qnames
                except AttributeError:
                    pass
                
                try:
                    if context.segDimValues:
                        dimensions = {}
                        for dim, mem in context.segDimValues.items():
                            try:
                                dim_name = dim.qname.localName
                                
                                if hasattr(mem, 'memberQname'):
                                    mem_name = mem.memberQname.localName
                                elif hasattr(mem, 'qname'):
                                    mem_name = mem.qname.localName
                                else:
                                    mem_name = "Unknown"
                                
                                dimensions[dim_name] = mem_name
                            except Exception:
                                continue
                        
                        if dimensions:
                            details['dimensions'] = dimensions
                except AttributeError:
                    pass
                    
            return details
            
        except Exception as e:
            self.logger.error(f"Error getting context details: {str(e)}")
            return {'error': str(e)}

    def _get_concept_details(self, concept):
        """Extract detailed information about a concept from the taxonomy"""
        if concept is None:
            return {}
        
        details = {
            'name': concept.qname.localName if hasattr(concept, 'qname') else None,
            'namespace': concept.qname.namespaceURI if hasattr(concept, 'qname') else None,
            'labels': {},
            'documentation': [],
            'relationships': {
                'parents': [],
                'children': []
            },
            'attributes': {
                'period_type': getattr(concept, 'periodType', None),
                'balance': getattr(concept, 'balance', None),
                'data_type': str(getattr(concept, 'typeQname', None)),
                'is_abstract': getattr(concept, 'isAbstract', False),
                'is_nillable': getattr(concept, 'isNillable', False)
            }
        }
        
        std_label = concept.label() if hasattr(concept, 'label') else None
        if std_label is not None:
            details['labels']['standard'] = std_label
        
        common_roles = [
            'http://www.xbrl.org/2003/role/label',
            'http://www.xbrl.org/2003/role/verboseLabel',
            'http://www.xbrl.org/2003/role/documentation'
        ]
        
        for role in common_roles:
            try:
                label = concept.label(role) if hasattr(concept, 'label') else None
                if label is not None and label != std_label:
                    role_name = role.split('/')[-1]
                    details['labels'][role_name] = label
            except Exception:
                continue
        
        if hasattr(concept, 'genDocs'):
            for doc in concept.genDocs:
                if hasattr(doc, 'value') and doc.value is not None:
                    cleaned = doc.value.strip()
                    if cleaned:
                        details['documentation'].append(cleaned)
        
        return details


    def get_validation_report(self):
        """Return a structured report of validation issues"""
        return {
            'total_errors': len(self.validation_errors),
            'errors': self.validation_errors,
            'severity_counts': {
                'critical': sum(1 for e in self.validation_errors if 'XML declaration' in e['message']),
                'common': sum(1 for e in self.validation_errors if 'Entity' in e['message']),
                'other': len(self.validation_errors) - sum(
                    1 for e in self.validation_errors if 'XML declaration' in e['message'] or 'Entity' in e['message'])
            }
        }

    def close(self):
        """Clean up resources"""
        if self.modelXbrl:
            self.cntlr.modelManager.close()
        if self.conn and not self.conn.closed:
            self.conn.close()
        self.modelXbrl = None
        self.fact_data = []
        self.grouped_facts = defaultdict(list)
        self.conn = None

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
    
    processor = XBRLProcessor(db_config=db_config)
    
    
    try:
        # Load an XBRL file
        submission_file = r"C:\Users\mithu\Documents\MEGA\Projects\Financial_Data_Analytics_Pipeline\filings\0000831259\10-Q\2014-03-31\0000831259-14-000022.txt"
        instance_file = r"C:\Users\mithu\Documents\MEGA\Projects\Financial_Data_Analytics_Pipeline\filings\0000831259\10-Q\2014-03-31\fcx-20140331.xml"
        
        if processor.load_xbrl_file(instance_file):
            # Store company metadata and get company ID
            company_id = processor.store_company_metadata(submission_file)
            
            # Process facts with the company ID
            fact_count = processor.process_facts(company_id=company_id)
            print(f"Processed {fact_count} facts")
            
            # Save to database
            processor.save_to_database(company_id)
            print("Successfully saved data to PostgreSQL database")
        else:
            print("Failed to load XBRL file")
    finally:
        processor.close()