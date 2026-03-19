from arelle import Cntlr, FileSource, ModelDocument
from collections import defaultdict
from urllib.parse import urljoin
import logging
import re
import psycopg2
from datetime import datetime 
from psycopg2 import sql
from psycopg2.extras import Json
from lxml import etree
from io import BytesIO
from bs4 import BeautifulSoup
import tempfile
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
        self.logger.setLevel(logging.INFO)  # Set the logging level

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
                    source_file VARCHAR(255),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create facts table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS xbrl_facts (
                    id SERIAL PRIMARY KEY,
                    company_id INTEGER REFERENCES company_metadata(id),
                    concept VARCHAR(255),
                    value NUMERIC,
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
    
    def extract_company_metadata(self, xbrl_file):
        """Extract company metadata from XBRL file"""
        with open(xbrl_file, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Initialize variables
        extracted_data = {
            'form_type': None,
            'filed_as_of_date': None,
            'industry': None,
            'sic': None,
            'accession_number': None,
            'company_name': None,
            'source_file': xbrl_file
        }
        
        # Extract Form Type
        form_match = re.search(r'CONFORMED SUBMISSION TYPE:\s+([^\n]+)', content)
        if form_match:
            extracted_data['form_type'] = form_match.group(1).strip()
        
        # Extract Filed As of Date
        date_match = re.search(r'FILED AS OF DATE:\s+(\d{8})', content)
        if date_match:
            date_str = date_match.group(1)
            extracted_data['filed_as_of_date'] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        
        # Extract and split SIC information
        sic_match = re.search(r'STANDARD INDUSTRIAL CLASSIFICATION:\s+([^\n]+)', content)
        if sic_match:
            sic_full = sic_match.group(1).strip()
            sic_parts = re.match(r'(.+?)\s*\[(\d+)\]', sic_full)
            if sic_parts:
                extracted_data['industry'] = sic_parts.group(1).strip().title()
                extracted_data['sic'] = sic_parts.group(2)
        
        # Extract Accession Number
        accession_match = re.search(r'ACCESSION NUMBER:\s+([^\n]+)', content)
        if accession_match:
            extracted_data['accession_number'] = accession_match.group(1).strip()
        
        # Extract and clean Company Name
        company_match = re.search(r'COMPANY CONFORMED NAME:\s+([^\n]+)', content)
        if company_match:
            cleaned_name = re.sub(r'[^a-zA-Z0-9\s]', '', company_match.group(1).strip())
            cleaned_name = re.sub(r'\s+', ' ', cleaned_name)
            extracted_data['company_name'] = cleaned_name
        
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
                WHERE accession_number = %s AND source_file = %s
            """, (data['accession_number'], data['source_file']))
            
            existing_record = cursor.fetchone()
            
            if existing_record:
                company_id = existing_record[0]
                self.logger.info(f"Using existing company record with ID: {company_id}")
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO company_metadata (
                        form_type, filed_as_of_date, industry, sic, 
                        accession_number, company_name, source_file
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    data['form_type'], data['filed_as_of_date'], data['industry'],
                    data['sic'], data['accession_number'], data['company_name'],
                    data['source_file']
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
            
            # Special handling for older filings
            load_options = {
                'disclosureSystem': 'efm-pragmatic' if int(self._filing_year(xbrl_file)) < 2018 
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
                
            
            if self.modelXbrl is None:
                self.logger.warning("Standard load failed, attempting fallback methods")
                # Try alternative loading methods
                return self._try_alternative_load_methods(xbrl_file)
                
            return True
            
        except Exception as e:
            self.logger.error(f"Critical error loading {xbrl_file}: {str(e)}")
            return False

    def _try_alternative_load_methods(self, xbrl_file):
        """Attempt alternative methods to load problematic XBRL files"""
        self.logger.info("Trying alternative loading methods")
        
        # Method 1: Try without validation
        try:
            self.modelXbrl = self.cntlr.modelManager.load(
                xbrl_file,
                validate=False,
                errorHandler=self._handle_validation_error
            )
            if self.modelXbrl:
                self.logger.warning("Loaded without validation - data quality may be compromised")
                return True
        except:
            pass
        
        # Method 2: Try as inline XBRL
        try:
            self.modelXbrl = self.cntlr.modelManager.load(
                xbrl_file,
                documentType=ModelDocument.Type.INLINEXBRL,
                errorHandler=self._handle_validation_error
            )
            if self.modelXbrl:
                self.logger.info("Successfully loaded as Inline XBRL")
                return True
        except:
            pass
        
        # Method 3: Try parsing as text and extracting XML
        try:
            with open(xbrl_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            xml_sections = re.findall(r'(<\?xml.*?</\w+>)', content, re.DOTALL)
            if xml_sections:
                self.logger.info(f"Found {len(xml_sections)} XML sections in file")
                for i, xml in enumerate(xml_sections):
                    try:
                        from io import StringIO
                        xml_io = StringIO(xml)
                        self.modelXbrl = self.cntlr.modelManager.load(
                            xml_io,
                            errorHandler=self._handle_validation_error
                        )
                        if self.modelXbrl:
                            self.logger.info(f"Successfully loaded XML section {i+1}")
                            return True
                    except:
                        continue
        except:
            pass
        

    def _is_complete_xbrl(self, content):
        """Check if the content appears to be a complete XBRL document"""
        if not content:
            return False
        # Basic check for opening and closing tags
        has_opening = ('<xbrl' in content.lower())
        has_closing = ('</xbrl>' in content.lower())
        return has_opening and has_closing

    def _load_from_string(self, xbrl_string, validate, strict_validation):
        """Load XBRL from a string"""
        try:
            # Create in-memory file source
            from arelle import FileSource
            file_source = FileSource.FileSource(BytesIO(xbrl_string.encode('utf-8')))
            
            # Load with the same options as original
            self.cntlr.modelManager.validateInferXbrl = validate
            self.cntlr.modelManager.validateDisclosureSystem = strict_validation
            
            self.modelXbrl = self.cntlr.modelManager.load(
                file_source,
                errorHandler=self._handle_validation_error
            )
            
            return self.modelXbrl is not None
            
        except Exception as e:
            self.logger.error(f"Error loading from string: {str(e)}")
            return False

    def _process_embedded_facts(self, skip_non_numeric, company_id):
        if not self.modelXbrl:
            return 0

        xbrl_contents = self._extract_all_embedded_xbrl(self.modelXbrl.modelDocument.uri)
        if not xbrl_contents:
            return 0

        # Get DTS from parent filing
        parent_dts = self._get_dts_for_year(self.modelXbrl)
        
        # Discover from fragments if needed
        if not parent_dts:
            parent_dts = self._discover_parent_dts(xbrl_contents)

        total_facts = 0
        for xbrl_content in xbrl_contents:
            facts_count = self._try_process_with_arelle(
                xbrl_content,
                skip_non_numeric,
                company_id,
                parent_dts=parent_dts
            )
            total_facts += facts_count if facts_count > 0 else \
                self._process_with_xml_fallback(xbrl_content, skip_non_numeric, company_id)

        return total_facts

    def _extract_year_from_xsd(self, xsd_path):
        """Extract year from XSD filename (handles both hyphenated and compact dates)"""
        try:
            filename = os.path.basename(xsd_path)
            
            # Updated pattern handles:
            # - scco-20231231.xsd
            # - us-gaap-2023-12-31.xsd
            # - dei_20221231.xsd
            year_match = re.search(r'''
                (?:^|[-_])              # Start or separator (- or _)
                (20\d{2})                # Year capture group
                (?:\d{4}|-\d{2}-\d{2})   # Either YYYYMMDD or -MM-DD
                \.xsd$                   # Extension
            ''', filename, re.VERBOSE)
            
            if year_match:
                year = int(year_match.group(1))
                if self._is_valid_taxonomy_year(year):
                    return year
                    
            raise ValueError(f"No valid year found in XSD filename: {filename}")
            
        except Exception as e:
            self.logger.error(f"XSD year extraction failed: {str(e)}")
            return None
        
    def _is_valid_taxonomy_year(self, year):
        """Validate taxonomy year is within reasonable bounds"""
        current_year = datetime.now().year
        return 2000 <= year <= current_year + 1  # +1 for early taxonomy adoptions

    def _get_taxonomy_urls_for_year(self, year):
        """Returns appropriate taxonomy URLs for a given year, with SEC-specific patterns."""
        try:
            year = int(year)
            if not (2000 <= year <= datetime.now().year + 1):
                raise ValueError(f"Invalid taxonomy year: {year}")
            
            # Common base URLs
            us_gaap_base = "http://xbrl.fasb.org/us-gaap/"
            dei_base = "http://xbrl.sec.gov/dei/"
            
            # Year-specific URL patterns
            if year >= 2020:
                return [
                    f"{us_gaap_base}{year}/elts/us-gaap-{year}-01-31.xsd",
                    f"{dei_base}{year}/dei-{year}-01-31.xsd"
                ]
            elif year >= 2017:
                return [
                    f"{us_gaap_base}{year}/elts/us-gaap-{year}-01-31.xsd",
                    f"{dei_base}{year}/dei-{year}-01-31.xsd",
                    "http://xbrl.sec.gov/currency/2020/currency-2020-01-31.xsd"
                ]
            elif year >= 2015:
                return [
                    f"{us_gaap_base}{year}/us-gaap-{year}-01-31.xsd",
                    f"{dei_base}{year}/dei-{year}-01-31.xsd",
                    "http://xbrl.sec.gov/currency/2015/currency-2015-01-31.xsd"
                ]
            else:  # Pre-2015 patterns
                return [
                    f"{us_gaap_base}{year}/us-gaap-{year}.xsd",
                    f"{dei_base}{year}/dei-{year}.xsd",
                    "http://xbrl.sec.gov/currency/2014/currency-2014.xsd"
                ]
                
        except Exception as e:
            self.logger.error(f"Taxonomy URL generation failed: {str(e)}")
            # Fallback to most recent taxonomies
            current_year = datetime.datetime.now().year
            return [
                f"http://xbrl.fasb.org/us-gaap/{current_year}/elts/us-gaap-{current_year}-01-31.xsd",
                f"http://xbrl.sec.gov/dei/{current_year}/dei-{current_year}-01-31.xsd"
            ]

    def _get_dts_for_year(self, model_xbrl):
        """Auto-discover year from XSD and load appropriate DTS"""
        try:
            # 1. Find the XSD file in the submission directory
            submission_dir = os.path.dirname(model_xbrl.modelDocument.uri)
            xsd_files = [f for f in os.listdir(submission_dir) 
                        if f.endswith('.xsd') and not f.startswith('._')]  # Skip macOS temp files
            
            if not xsd_files:
                raise FileNotFoundError("No XSD files found in submission directory")
                
            # 2. Extract year from first valid XSD
            for xsd in xsd_files:
                year = self._extract_year_from_xsd(os.path.join(submission_dir, xsd))
                if year:
                    break
                    
            if not year:
                raise ValueError("No XSD with detectable year found")
                
            self.logger.info(f"Detected taxonomy year: {year} from {xsd}")
            
            # 3. Load DTS with proper taxonomies
            return self.cntlr.modelManager.load(
                FileSource.openFileSource(model_xbrl.modelDocument.uri, self.cntlr),
                taxonomyURLs=self._get_taxonomy_urls_for_year(year),
                validate=False
            )
            
        except Exception as e:
            self.logger.error(f"DTS loading failed: {str(e)}")
            return None
        
    def _discover_parent_dts(self, xbrl_contents):
        """Find the first valid DTS from fragments."""
        for xbrl_content in xbrl_contents:
            try:
                with tempfile.NamedTemporaryFile(mode='w+', suffix='.xbrl', delete=False) as tmp:
                    tmp.write(xbrl_content)
                    tmp_path = tmp.name

                model_xbrl = self.cntlr.modelManager.load(
                    FileSource.openFileSource(tmp_path, self.cntlr),
                    disclosureSystem='efm-pragmatic',
                    validate=False,
                    loadDTS=True
                )
                
                dts = self._get_dts(model_xbrl)
                if dts:
                    model_xbrl.close()
                    os.unlink(tmp_path)
                    return dts
                    
                os.unlink(tmp_path)
            except Exception as e:
                self.logger.warning(f"DTS discovery attempt failed: {str(e)}")
                continue
        return None

    def _extract_all_embedded_xbrl(self, xbrl_file):
        """Return ALL embedded XBRL sections found in file"""
        try:
            with open(xbrl_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            xbrl_sections = []
            
            # Find all XBRL sections using various patterns
            patterns = [
                r'(<xbrl[\s>].*?</xbrl>)',
                r'(<XBRL[\s>].*?</XBRL>)',
                r'(<html>.*?<xbrl>.*?</xbrl>.*?</html>)',
                r'(<\?xml.*?<.*?xbrli:.*?</.*?>)'
            ]
            
            for pattern in patterns:
                for match in re.finditer(pattern, content, re.DOTALL):
                    if self._is_complete_xbrl(match.group(1)):
                        xbrl_sections.append(match.group(1))
            
            # Additional BeautifulSoup search if no matches yet
            if not xbrl_sections:
                soup = BeautifulSoup(content, 'html.parser')
                for tag in soup.find_all(['xbrl', 'XBRL']):
                    tag_str = str(tag)
                    if self._is_complete_xbrl(tag_str):
                        xbrl_sections.append(tag_str)
            
            return xbrl_sections if xbrl_sections else None
            
        except Exception as e:
            self.logger.error(f"Error searching for embedded XBRL: {str(e)}")
            return None

    def _try_process_with_arelle(self, xbrl_content, skip_non_numeric, company_id, parent_dts=None):
        """Process a fragment with either a provided DTS or hardcoded taxonomies."""
        try:
            with tempfile.NamedTemporaryFile(mode='w+', suffix='.xbrl', delete=False) as tmp:
                tmp.write(xbrl_content)
                tmp_path = tmp.name

            load_options = {
                'disclosureSystem': 'efm-pragmatic',
                'validate': False,
                'loadDTS': True if not parent_dts else False,  # Skip DTS load if parent_dts provided
                'dts': parent_dts  # Use provided DTS (or None)
            }

            # Fallback to hardcoded taxonomies if no parent_dts
            if not parent_dts:
                load_options['taxonomyURLs'] = [
                    "http://xbrl.fasb.org/us-gaap/2023/elts/us-gaap-2023-01-31.xsd",
                    "http://xbrl.sec.gov/dei/2023/dei-2023-01-31.xsd"
                ]

            model_xbrl = self.cntlr.modelManager.load(
                FileSource.openFileSource(tmp_path, self.cntlr),
                **load_options
            )
            if model_xbrl and model_xbrl.facts:
                return self._process_facts(model_xbrl, skip_non_numeric, company_id)
            return 0
        finally:
            os.unlink(tmp_path)

    def _filing_year(self, xbrl_file):
        """Attempt to guess filing year from filename or content"""
        try:
            # Try from filename (common patterns)
            year_match = re.search(r'(19|20)\d{2}', xbrl_file)
            if year_match:
                return year_match.group(0)
            
            # Try from file content
            with open(xbrl_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read(5000)  # Only read first part
                
            # Look for CONFORMED PERIOD OF REPORT
            report_match = re.search(r'CONFORMED PERIOD OF REPORT:\s*(\d{8})', content)
            if report_match:
                return report_match.group(1)[:4]
                
            # Look for dates in header
            date_match = re.search(r'(19|20)\d{6}', content)
            if date_match:
                return date_match.group(0)[:4]
                
            return str(datetime.now().year - 5)  # Default to "old" filing
        except:
            return str(datetime.now().year - 5)  # Default to "old" filing


    def process_facts(self, skip_non_numeric=True, company_id=None):
        """
        Process facts with multiple fallback strategies in this order:
        1. Standard XBRL fact processing
        2. Embedded XBRL processing
        3. lxml parsing fallback
        4. Raw text pattern matching
        Returns count of successfully processed facts
        """
        if not self.modelXbrl:
            self.logger.error("No XBRL model loaded - cannot process facts")
            return 0
            
        self.fact_data = []
        strategies = [
            ("Standard XBRL processing", self._process_standard_facts),
            ("Embedded XBRL processing", self._process_embedded_facts),
            ("lxml fallback parsing", self._process_with_xml_fallback),
            ("Raw text extraction", self._extract_from_raw_text)
        ]

        for strategy_name, strategy_func in strategies:
            try:
                self.logger.info(f"Attempting {strategy_name}")
                fact_count = strategy_func(skip_non_numeric, company_id)
                
                if fact_count > 0:
                    self.logger.info(f"{strategy_name} found {fact_count} facts")
                    return fact_count
                    
            except Exception as e:
                self.logger.warning(f"{strategy_name} failed: {str(e)}")
                continue

        self.logger.error("All fact extraction strategies failed")
        return 0
    
    def _process_with_xml_fallback(self, xbrl_content, skip_non_numeric, company_id):
        """Fallback processing using direct XML parsing with complete context"""
        try:
            # Parse with recovery mode for malformed XML
            parser = etree.XMLParser(recover=True, remove_comments=True, resolve_entities=False)
            tree = etree.parse(BytesIO(xbrl_content.encode('utf-8')), parser)
            
            # Dynamic namespace detection
            nsmap = self._detect_namespaces(tree)
            
            # Extract all contexts first
            context_map = self._extract_complete_contexts(tree, nsmap)
            
            # Extract concept details from schema if available
            concept_details_map = self._extract_complete_concept_details(tree, nsmap)
            
            facts = []
            for elem in tree.xpath('//*[text() and not(contains(local-name(), "Context"))]', namespaces=nsmap):
                try:
                    concept = etree.QName(elem).localname
                    context_ref = elem.get('contextRef')
                    unit_ref = elem.get('unitRef', 'unitless')
                    
                    # Get enriched context
                    context_details = context_map.get(context_ref, {})
                    
                    # Get enriched concept details
                    concept_detail = concept_details_map.get(concept, {
                        'source': 'embedded_xbrl',
                        'labels': {'standard': concept},
                        'namespace': etree.QName(elem).namespace
                    })
                    
                    # Handle numeric values
                    text_value = elem.text.strip().replace(',', '')
                    try:
                        value = float(text_value)
                        is_numeric = True
                    except ValueError:
                        if skip_non_numeric:
                            continue
                        value = text_value
                        is_numeric = False
                    
                    # Build complete fact entry
                    fact_entry = {
                        'company_id': company_id,
                        'concept': concept,
                        'value': value,
                        'unit': unit_ref,
                        'context': context_details,
                        'concept_details': concept_detail
                    }
                    
                    # Add numeric-specific attributes
                    if is_numeric:
                        fact_entry['concept_details']['attributes'] = {
                            'decimals': elem.get('decimals'),
                            'precision': elem.get('precision')
                        }
                    
                    facts.append(fact_entry)
                
                except Exception as e:
                    self.logger.warning(f"Error processing element {elem.tag}: {str(e)}")
                    continue
            
            self.fact_data.extend(facts)
            return len(facts)
        
        except Exception as e:
            self.logger.error(f"XML fallback processing failed: {str(e)}", exc_info=True)
            return 0

    def _detect_namespaces(self, tree):
        """Dynamically detect namespaces from the document"""
        nsmap = {
            'xbrli': 'http://www.xbrl.org/2003/instance',
            'link': 'http://www.xbrl.org/2003/linkbase',
            'xlink': 'http://www.w3.org/1999/xlink',
            'xbrldi': 'http://xbrl.org/2005/xbrldi'
        }
        
        # Detect additional namespaces from root element
        root = tree.getroot()
        for prefix, uri in root.nsmap.items():
            if prefix and uri:
                nsmap[prefix] = uri
        
        return nsmap

    def _verify_namespaces(self, tree):
        """Log all namespaces present in the document"""
        root = tree.getroot()
        self.logger.info("Document Namespaces:")
        for prefix, uri in root.nsmap.items():
            self.logger.info(f"{prefix}: {uri}")
        
        # Specifically check for dimensions namespace
        xbrldi_ns = [uri for uri in root.nsmap.values() if 'xbrldi' in uri]
        if not xbrldi_ns:
            self.logger.warning("No xbrldi namespace found in document")
        else:
            self.logger.info(f"Found xbrldi namespace: {xbrldi_ns[0]}")
    
    def _check_alternative_dimension_locations(self, tree, nsmap):
        """Check for dimensions in non-standard locations"""
        # Check for dimensions directly under context
        direct_dims = tree.xpath('//xbrli:context/xbrldi:explicitMember', namespaces=nsmap)
        self.logger.info(f"Found {len(direct_dims)} dimensions directly under context")
        
        # Check for dimensions in extension elements
        ext_dims = tree.xpath('//*[contains(local-name(), "Extension")]//xbrldi:explicitMember', namespaces=nsmap)
        self.logger.info(f"Found {len(ext_dims)} dimensions in extension elements")
        
        # Check for dimensions in footnotes
        footnote_dims = tree.xpath('//xbrli:footnote//xbrldi:explicitMember', namespaces=nsmap)
        self.logger.info(f"Found {len(footnote_dims)} dimensions in footnotes")

    def _extract_complete_contexts(self, tree, nsmap):
        """Extract complete context information with integrated dimension extraction"""
        # Log sample context XML for debugging
        sample_context = tree.xpath('//xbrli:context[1]', namespaces=nsmap)
        if sample_context:
            self.logger.debug(f"Sample Context XML:\n{etree.tostring(sample_context[0], pretty_print=True, encoding='unicode')}")

        # First get all dimensions from the document using comprehensive extractor
        dimension_data = self.extract_dimensions(tree)
        
        context_map = {}
        
        for context in tree.xpath('//xbrli:context', namespaces=nsmap):
            context_id = context.get('id')
            if not context_id:
                continue
                
            context_details = {
                'entity': {},
                'period': {},
                'dimensions': {}
            }
            
            # Entity information (unchanged)
            entity = context.find('xbrli:entity', namespaces=nsmap)
            if entity is not None:
                identifier = entity.find('xbrli:identifier', namespaces=nsmap)
                if identifier is not None:
                    context_details['entity'] = {
                        'identifier': identifier.text,
                        'scheme': identifier.get('scheme', '')
                    }
            
            # Period information (unchanged)
            period = context.find('xbrli:period', namespaces=nsmap)
            if period is not None:
                start_date = period.find('xbrli:startDate', namespaces=nsmap)
                end_date = period.find('xbrli:endDate', namespaces=nsmap)
                instant = period.find('xbrli:instant', namespaces=nsmap)
                
                if start_date is not None and end_date is not None:
                    context_details['period'] = {
                        'start': start_date.text,
                        'end': end_date.text,
                        'type': 'duration'
                    }
                elif instant is not None:
                    context_details['period'] = {
                        'instant': instant.text,
                        'type': 'instant'
                    }
            
            # Get dimensions for this context from our comprehensive extraction
            if context_id in dimension_data:
                # Merge all dimension types into our standard dimensions field
                for dim_type, dimensions in dimension_data[context_id].items():
                    for dim_name, dim_value in dimensions.items():
                        clean_name = self._clean_dimension_name(dim_name)
                        if clean_name and dim_value:
                            context_details['dimensions'][clean_name] = dim_value
            
            context_map[context_id] = context_details
        
        return context_map

    def extract_dimensions(self, tree):
        """Comprehensive dimension extraction from all possible locations"""
        nsmap = self._get_complete_nsmap(tree)
        dimension_data = {}
        
        for context in tree.xpath('//xbrli:context', namespaces=nsmap):
            context_id = context.get('id')
            if not context_id:
                continue
                
            dimension_data[context_id] = {
                'explicit': {},
                'typed': {},
                'segment': {},
                'scenario': {},
                'fact': {}
            }
            
            # Check standard dimension locations
            self._get_standard_dimensions(context, dimension_data[context_id], nsmap)
            
            # Check non-standard locations
            self._get_nonstandard_dimensions(context, dimension_data[context_id], nsmap)
            
            # Check fact dimensions
            self._get_fact_dimensions(tree, context, dimension_data[context_id], nsmap)
        
        return dimension_data

    def _get_standard_dimensions(self, context, context_data, nsmap):
        """Extract dimensions from standard locations"""
        # Process segment dimensions
        segment = context.find('xbrli:entity/xbrli:segment', namespaces=nsmap)
        if segment is not None:
            for dim in segment.xpath('.//xbrldi:explicitMember', namespaces=nsmap):
                dim_name = dim.get('dimension')
                context_data['segment'][dim_name] = dim.text
            
            for dim in segment.xpath('.//xbrldi:typedMember', namespaces=nsmap):
                dim_name = dim.get('dimension')
                context_data['segment'][dim_name] = dim[0].text if len(dim) > 0 else ''
        
        # Process scenario dimensions
        scenario = context.find('xbrli:scenario', namespaces=nsmap)
        if scenario is not None:
            for dim in scenario.xpath('.//xbrldi:explicitMember', namespaces=nsmap):
                dim_name = dim.get('dimension')
                context_data['scenario'][dim_name] = dim.text
            
            for dim in scenario.xpath('.//xbrldi:typedMember', namespaces=nsmap):
                dim_name = dim.get('dimension')
                context_data['scenario'][dim_name] = dim[0].text if len(dim) > 0 else ''

    def _get_nonstandard_dimensions(self, context, context_data, nsmap):
        """Extract dimensions from non-standard locations"""
        # Direct context children
        for dim in context.xpath('./*[contains(local-name(), "Member")]'):
            dim_name = dim.get('dimension') or dim.tag
            context_data['explicit'][dim_name] = dim.text
        
        # Extension elements
        for ext in context.xpath('.//*[contains(local-name(), "Extension")]'):
            for dim in ext.xpath('.//*[contains(local-name(), "Member")]'):
                dim_name = dim.get('dimension') or dim.tag
                context_data['explicit'][dim_name] = dim.text
        
        # Dimension attributes
        for attr, value in context.attrib.items():
            if any(keyword in attr for keyword in ['Axis', 'Member', 'Dimension']):
                context_data['explicit'][attr] = value

    def _get_fact_dimensions(self, tree, context, context_data, nsmap):
        """Extract dimensions from facts referencing this context"""
        context_id = context.get('id')
        if not context_id:
            return
        
        # Check facts using the context reference
        facts = tree.xpath(f'//*[@contextRef="{context_id}"]', namespaces=nsmap)
        for fact in facts[:10]:  # Check first 10 facts to avoid performance hit
            for attr, value in fact.attrib.items():
                if any(keyword in attr for keyword in ['Axis', 'Member', 'Dimension']):
                    context_data['fact'][attr] = value

    def _get_complete_nsmap(self, tree):
        """Build complete namespace map including dimension namespaces"""
        nsmap = {
            'xbrli': 'http://www.xbrl.org/2003/instance',
            'link': 'http://www.xbrl.org/2003/linkbase',
            'xlink': 'http://www.w3.org/1999/xlink',
            'xbrldi': 'http://xbrl.org/2005/xbrldi',
            'xbrldt': 'http://xbrl.org/2005/xbrldt'
        }
        
        # Add document-specific namespaces
        root = tree.getroot()
        for prefix, uri in root.nsmap.items():
            if prefix:
                nsmap[prefix] = uri
        
        return nsmap

    def _clean_dimension_name(self, dim_name):
        """Normalize dimension names for consistent output"""
        if not dim_name:
            return None
        
        # Remove namespace prefixes
        if ':' in dim_name:
            dim_name = dim_name.split(':')[-1]
        
        # Standardize naming conventions
        dim_name = (dim_name.replace('axis', 'Axis')
                    .replace('member', 'Member')
                    .replace('Dimension', ''))
        
        return dim_name.strip()

    # def _extract_all_dimensions(self, tree, nsmap):
    #     """Extract dimensions from all possible locations with robust namespace handling"""
    #     # First ensure we have all required namespaces
    #     full_nsmap = self._prepare_namespaces(tree, nsmap)
        
    #     context_map = {}
        
    #     for context in tree.xpath('//xbrli:context', namespaces=full_nsmap):
    #         context_id = context.get('id')
    #         if not context_id:
    #             continue
                
    #         context_details = {
    #             'entity': {},
    #             'period': {},
    #             'dimensions': {}
    #         }
            
    #         # 1. Standard dimension locations (segment/scenario)
    #         self._process_standard_dimensions(context, context_details, full_nsmap)
            
    #         # 2. Alternative dimension locations
    #         self._process_alternative_dimensions(context, context_details, full_nsmap)
            
    #         # 3. Fallback: Direct children of context
    #         self._process_direct_dimensions(context, context_details, full_nsmap)
            
    #         context_map[context_id] = context_details
        
    #     return context_map

    # def _prepare_namespaces(self, tree, nsmap):
    #     """Ensure all required namespaces are present"""
    #     full_nsmap = nsmap.copy()
        
    #     # Add common dimension namespaces if missing
    #     required_ns = {
    #         'xbrldi': 'http://xbrl.org/2005/xbrldi',
    #         'xbrldt': 'http://xbrl.org/2005/xbrldt',
    #         'dim': 'http://xbrl.org/2006/dimensions'
    #     }
        
    #     # Get namespaces from document root
    #     root = tree.getroot()
    #     for prefix, uri in root.nsmap.items():
    #         if prefix:
    #             full_nsmap[prefix] = uri
        
    #     # Add missing required namespaces
    #     for prefix, uri in required_ns.items():
    #         if prefix not in full_nsmap:
    #             full_nsmap[prefix] = uri
        
    #     return full_nsmap

    # def _process_standard_dimensions(self, context, context_details, nsmap):
    #     """Process dimensions in standard segment/scenario locations"""
    #     # Check segment first
    #     segment = context.find('xbrli:entity/xbrli:segment', namespaces=nsmap)
    #     if segment is not None:
    #         self._extract_dimensions_from_element(segment, context_details, nsmap)
        
    #     # Check scenario
    #     scenario = context.find('xbrli:scenario', namespaces=nsmap)
    #     if scenario is not None:
    #         self._extract_dimensions_from_element(scenario, context_details, nsmap)

    # def _extract_dimensions_from_element(self, element, context_details, nsmap):
    #     """Extract dimensions from any parent element"""
    #     # Explicit dimensions (most common)
    #     for dim in element.xpath('.//xbrldi:explicitMember', namespaces=nsmap):
    #         self._add_dimension(context_details, dim.get('dimension'), dim.text)
        
    #     # Typed dimensions
    #     for dim in element.xpath('.//xbrldi:typedMember', namespaces=nsmap):
    #         dim_name = dim.get('dimension')
    #         dim_value = dim[0].text if len(dim) > 0 else ''
    #         self._add_dimension(context_details, dim_name, dim_value)
        
    #     # Non-namespaced dimensions (some legacy filings)
    #     for dim in element.xpath('.//*[local-name()="explicitMember"]'):
    #         self._add_dimension(context_details, dim.get('dimension'), dim.text)

    # def _process_alternative_dimensions(self, context, context_details, nsmap):
    #     """Check for dimensions in non-standard locations"""
    #     # Dimensions as direct children of context
    #     for dim in context.xpath('./*[local-name()="explicitMember"]'):
    #         self._add_dimension(context_details, dim.get('dimension'), dim.text)
        
    #     # Dimensions in extension elements
    #     for ext in context.xpath('.//*[contains(local-name(), "Extension")]'):
    #         for dim in ext.xpath('.//*[contains(local-name(), "Member")]'):
    #             self._add_dimension(context_details, dim.get('dimension'), dim.text)

    # def _process_direct_dimensions(self, context, context_details, nsmap):
    #     """Final fallback - look for dimension-like attributes"""
    #     # Check for dimension information in context attributes
    #     for attr in context.attrib:
    #         if any(keyword in attr for keyword in ['dimension', 'Axis', 'Member']):
    #             self._add_dimension(context_details, attr, context.get(attr))

    # def _add_dimension(self, context_details, dim_name, dim_value):
    #     """Safely add a dimension after cleaning"""
    #     if not dim_name or not dim_value:
    #         return
        
    #     # Clean dimension name
    #     if ':' in dim_name:
    #         dim_name = dim_name.split(':')[-1]
        
    #     # Clean dimension value
    #     dim_value = str(dim_value).strip()
    #     if not dim_value:
    #         return
        
    #     # Add to context
    #     context_details['dimensions'][dim_name] = dim_value

    # def _log_dimension_debug_info(self, tree, nsmap):
    #     """Log comprehensive dimension debugging information"""
    #     self.logger.info("=== Dimension Debugging Information ===")
        
    #     # 1. Verify namespaces
    #     self._log_namespace_info(tree)
        
    #     # 2. Check for any dimension-like elements
    #     self._log_all_dimension_elements(tree, nsmap)
        
    #     # 3. Examine context structure
    #     self._log_context_structure(tree, nsmap)

    def _log_namespace_info(self, tree):
        """Log all namespaces found in document"""
        root = tree.getroot()
        self.logger.info("Document Namespaces:")
        for prefix, uri in root.nsmap.items():
            self.logger.info(f"{prefix}: {uri}")

    # def _log_all_dimension_elements(self, tree, nsmap):
    #     """Log all elements that might contain dimensions"""
    #     # Standard dimension elements
    #     explicit = tree.xpath('//xbrldi:explicitMember', namespaces=nsmap)
    #     typed = tree.xpath('//xbrldi:typedMember', namespaces=nsmap)
        
    #     # Non-namespaced variants
    #     no_ns_explicit = tree.xpath('//*[local-name()="explicitMember"]')
    #     no_ns_typed = tree.xpath('//*[local-name()="typedMember"]')
        
    #     # Dimension-like elements
    #     axis_elements = tree.xpath('//*[contains(local-name(), "Axis")]')
    #     member_elements = tree.xpath('//*[contains(local-name(), "Member")]')
        
    #     self.logger.info(f"Found {len(explicit)} explicitMember elements")
    #     self.logger.info(f"Found {len(typed)} typedMember elements")
    #     self.logger.info(f"Found {len(no_ns_explicit)} non-namespaced explicitMember")
    #     self.logger.info(f"Found {len(no_ns_typed)} non-namespaced typedMember")
    #     self.logger.info(f"Found {len(axis_elements)} *Axis* elements")
    #     self.logger.info(f"Found {len(member_elements)} *Member* elements")
        
    #     # Log sample if found
    #     if explicit:
    #         self.logger.debug(f"Sample explicitMember: {etree.tostring(explicit[0])}")

    def _log_context_structure(self, tree, nsmap):
        """Log the structure of the first context"""
        context = tree.xpath('//xbrli:context[1]', namespaces=nsmap)
        if context:
            self.logger.info("Structure of first context:")
            for child in context[0]:
                self.logger.info(f"- {child.tag}: {len(child)} children")
                for subchild in child:
                    self.logger.info(f"  - {subchild.tag}")
    
    def _log_dimension_debug_info(self, tree, nsmap):
        """Log comprehensive dimension debugging information"""
        self.logger.info("=== Dimension Debugging Information ===")
        
        # 1. Verify namespaces
        self._log_namespace_info(tree)
        
        # 2. Check for any dimension-like elements
        self._log_all_dimension_elements(tree, nsmap)
        
        # 3. Examine context structure
        self._log_context_structure(tree, nsmap)

    def _log_namespace_info(self, tree):
        """Log all namespaces found in document"""
        root = tree.getroot()
        self.logger.info("Document Namespaces:")
        for prefix, uri in root.nsmap.items():
            self.logger.info(f"{prefix}: {uri}")

    def _log_all_dimension_elements(self, tree, nsmap):
        """Log all elements that might contain dimensions"""
        # Standard dimension elements
        explicit = tree.xpath('//xbrldi:explicitMember', namespaces=nsmap)
        typed = tree.xpath('//xbrldi:typedMember', namespaces=nsmap)
        
        # Non-namespaced variants
        no_ns_explicit = tree.xpath('//*[local-name()="explicitMember"]')
        no_ns_typed = tree.xpath('//*[local-name()="typedMember"]')
        
        # Dimension-like elements
        axis_elements = tree.xpath('//*[contains(local-name(), "Axis")]')
        member_elements = tree.xpath('//*[contains(local-name(), "Member")]')
        
        self.logger.info(f"Found {len(explicit)} explicitMember elements")
        self.logger.info(f"Found {len(typed)} typedMember elements")
        self.logger.info(f"Found {len(no_ns_explicit)} non-namespaced explicitMember")
        self.logger.info(f"Found {len(no_ns_typed)} non-namespaced typedMember")
        self.logger.info(f"Found {len(axis_elements)} *Axis* elements")
        self.logger.info(f"Found {len(member_elements)} *Member* elements")
        
        # Log sample if found
        if explicit:
            self.logger.debug(f"Sample explicitMember: {etree.tostring(explicit[0])}")

    def _log_context_structure(self, tree, nsmap):
        """Log the structure of the first context"""
        context = tree.xpath('//xbrli:context[1]', namespaces=nsmap)
        if context:
            self.logger.info("Structure of first context:")
            for child in context[0]:
                self.logger.info(f"- {child.tag}: {len(child)} children")
                for subchild in child:
                    self.logger.info(f"  - {subchild.tag}")

    def _extract_complete_concept_details(self, tree, nsmap):
        """Extract comprehensive concept details including labels, attributes and relationships"""
        concept_details = {}
        
        # Extract from label linkbase if available
        for label in tree.xpath('//link:label', namespaces=nsmap):
            label_name = label.get('{http://www.w3.org/1999/xlink}label')
            if label_name:
                if label_name not in concept_details:
                    concept_details[label_name] = {'labels': {}}
                role = label.get('{http://www.w3.org/1999/xlink}role', '').split('/')[-1] or 'standard'
                concept_details[label_name]['labels'][role] = label.text
        
        # Extract from concept definitions in schema
        schema_ns = {'xsd': 'http://www.w3.org/2001/XMLSchema'}
        for element in tree.xpath('//xsd:element', namespaces=schema_ns):
            concept = element.get('name')
            if concept:
                if concept not in concept_details:
                    concept_details[concept] = {
                        'labels': {'standard': concept},
                        'attributes': {},
                        'relationships': {'parents': [], 'children': []}
                    }
                
                # Basic attributes
                attrs = concept_details[concept]['attributes']
                attrs.update({
                    'type': element.get('type'),
                    'substitutionGroup': element.get('substitutionGroup'),
                    'balance': self._get_balance_type(element.get('type')),
                    'is_abstract': element.get('abstract', 'false').lower() == 'true',
                    'is_nillable': element.get('nillable', 'false').lower() == 'true',
                    'period_type': 'duration' if 'duration' in (element.get('type') or '').lower() else 'instant'
                })
                
                # Documentation
                annotation = element.find('xsd:annotation', namespaces=schema_ns)
                if annotation is not None:
                    docs = []
                    for doc in annotation.findall('xsd:documentation', namespaces=schema_ns):
                        if doc.text and doc.text.strip():
                            docs.append(doc.text.strip())
                    if docs:
                        concept_details[concept]['documentation'] = docs
        
        # Extract presentation relationships
        for arc in tree.xpath('//link:presentationArc', namespaces=nsmap):
            from_concept = arc.get('{http://www.w3.org/1999/xlink}from')
            to_concept = arc.get('{http://www.w3.org/1999/xlink}to')
            
            if from_concept and to_concept:
                if from_concept not in concept_details:
                    concept_details[from_concept] = self._create_default_concept(from_concept)
                if 'children' not in concept_details[from_concept]['relationships']:
                    concept_details[from_concept]['relationships']['children'] = []
                concept_details[from_concept]['relationships']['children'].append(to_concept)
                
                if to_concept not in concept_details:
                    concept_details[to_concept] = self._create_default_concept(to_concept)
                if 'parents' not in concept_details[to_concept]['relationships']:
                    concept_details[to_concept]['relationships']['parents'] = []
                concept_details[to_concept]['relationships']['parents'].append(from_concept)
        
        return concept_details

    def _create_default_concept(self, concept_name):
        """Create a default concept structure"""
        return {
            'labels': {'standard': concept_name},
            'attributes': {
                'type': None,
                'balance': None,
                'is_abstract': False,
                'is_nillable': True,
                'period_type': 'duration'
            },
            'relationships': {
                'parents': [],
                'children': []
            },
            'source': 'inferred'
        }

    def _get_balance_type(self, type_name):
        """Determine balance type from concept data type"""
        if not type_name:
            return None
        type_name = type_name.lower()
        if 'debit' in type_name:
            return 'debit'
        if 'credit' in type_name:
            return 'credit'
        if 'monetary' in type_name:
            return 'monetary'
        return None


    def _process_context(self, context, nsmap):
        """Full context processing with detailed logging"""
        context_details = {
            'entity': {},
            'period': {},
            'dimensions': {}
        }
        
        # Log context ID for tracking
        context_id = context.get('id')
        self.logger.debug(f"Processing context: {context_id}")
        
        # Process entity
        entity = context.find('xbrli:entity', namespaces=nsmap)
        if entity is not None:
            identifier = entity.find('xbrli:identifier', namespaces=nsmap)
            if identifier is not None:
                context_details['entity'] = {
                    'identifier': identifier.text,
                    'scheme': identifier.get('scheme', '')
                }
            
            # Segment dimensions
            segment = entity.find('xbrli:segment', namespaces=nsmap)
            if segment is not None:
                self._extract_dimensions(segment, context_details, nsmap)
        
        # Process period
        period = context.find('xbrli:period', namespaces=nsmap)
        if period is not None:
            start_date = period.find('xbrli:startDate', namespaces=nsmap)
            end_date = period.find('xbrli:endDate', namespaces=nsmap)
            instant = period.find('xbrli:instant', namespaces=nsmap)
            
            if start_date is not None and end_date is not None:
                context_details['period'] = {
                    'start': start_date.text,
                    'end': end_date.text,
                    'type': 'duration'
                }
            elif instant is not None:
                context_details['period'] = {
                    'instant': instant.text,
                    'type': 'instant'
                }
        
        
        # Dimension extraction with fallbacks
        if entity is not None:
            segment = entity.find('xbrli:segment', namespaces=nsmap)
            if segment is not None:
                self.logger.debug(f"Found segment in context {context_id}")
                self._extract_dimensions(segment, context_details, nsmap)
            else:
                self.logger.debug(f"No segment found in context {context_id}")
        
        scenario = context.find('xbrli:scenario', namespaces=nsmap)
        if scenario is not None:
            self.logger.debug(f"Found scenario in context {context_id}")
            self._extract_dimensions(scenario, context_details, nsmap)
        else:
            self.logger.debug(f"No scenario found in context {context_id}")
        
        # Check if we found any dimensions
        if not context_details['dimensions']:
            self.logger.debug(f"No dimensions found in context {context_id}")
            # Try alternative dimension locations
            self._check_alternative_dimension_locations_in_context(context, nsmap, context_details)
        
        return context_details

    # def _extract_from_raw_text(self, company_id):
    #     """Last-resort text pattern matching"""
    #     try:
    #         with open(self.modelXbrl.modelDocument.uri, 'r', encoding='utf-8', errors='ignore') as f:
    #             content = f.read()
                
    #         # Match both the pattern and capture the preceding label
    #         pattern = r'([A-Za-z][^:$]*?)\$?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)'
    #         matches = re.finditer(pattern, content)
            
    #         fact_count = 0
    #         for match in matches:
    #             try:
    #                 self.fact_data.append({
    #                     'company_id': company_id,
    #                     'concept': match.group(1).strip()[:100],  # Truncate long labels
    #                     'value': float(match.group(2).replace(',', '')),
    #                     'unit': 'USD',
    #                     'context': {'source': 'text_pattern'},
    #                     'concept_details': {'extraction_method': 'regex'}
    #                 })
    #                 fact_count += 1
    #                 if fact_count >= 1000:  # Safety limit
    #                     break
    #             except:
    #                 continue
                    
    #         return fact_count
    #     except Exception as e:
    #         self.logger.error(f"Raw text extraction failed: {str(e)}")
    #         return 0

    def _extract_from_raw_text(self, company_id):
        """Last-resort attempt to extract data from raw file text"""
        fact_count = 0
        try:
            if not hasattr(self.modelXbrl, 'modelDocument') or not self.modelXbrl.modelDocument:
                return 0
                
            file_path = self.modelXbrl.modelDocument.uri
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Look for common financial patterns
            patterns = [
                (r'<([^>]+)>\s*([\d,\.]+)\s*</\1>', 'tag_value'),  # <tag>123</tag>
                (r'(\w+)\s*[:=]\s*([\d,\.]+)', 'key_value'),        # key: 123 or key=123
                (r'\$?\d{1,3}(?:,\d{3})*(?:\.\d+)?', 'numeric')     # Standalone numbers
            ]
            
            for pattern, pattern_type in patterns:
                matches = re.finditer(pattern, content)
                for match in matches:
                    try:
                        if pattern_type == 'tag_value':
                            concept = match.group(1)
                            value = match.group(2)
                        elif pattern_type == 'key_value':
                            concept = match.group(1)
                            value = match.group(2)
                        else:  # numeric
                            concept = 'extracted_number'
                            value = match.group(0)
                        
                        # Clean the value
                        clean_value = float(value.replace('$', '').replace(',', ''))
                        
                        fact_entry = {
                            'company_id': company_id,
                            'concept': concept,
                            'value': clean_value,
                            'unit': 'USD',
                            'context': {'period': 'unknown', 'source': 'raw_text_extraction'},
                            'concept_details': {'extraction_method': pattern_type}
                        }
                        
                        self.fact_data.append(fact_entry)
                        fact_count += 1
                        
                        # Limit to 1000 facts to avoid memory issues
                        if fact_count >= 1000:
                            break
                            
                    except (ValueError, AttributeError):
                        continue
                
                if fact_count > 0:
                    self.logger.warning(f"Extracted {fact_count} facts using {pattern_type} pattern")
                    break
                    
        except Exception as e:
            self.logger.error(f"Error in raw text extraction: {str(e)}")
            
        return fact_count

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
                    numeric_value = float(fact.value)
                except (ValueError, TypeError):
                    if skip_non_numeric:
                        continue
                    numeric_value = fact.value
                
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
                    'value': numeric_value,
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

    def _try_alternative_fact_extraction(self, company_id):
        """Attempt alternative methods to extract facts from problematic filings"""
        fact_count = 0
        
        # Method 1: Try getting facts from footnotes
        if hasattr(self.modelXbrl, 'footnotes'):
            self.logger.info("Attempting to extract facts from footnotes")
            for note in self.modelXbrl.footnotes:
                try:
                    # Look for numeric values in footnotes
                    num_matches = re.findall(r'(\$?\d{1,3}(?:,\d{3})*(?:\.\d+)?)', note.value)
                    if num_matches:
                        fact_entry = {
                            'company_id': company_id,
                            'concept': 'extracted_from_footnote',
                            'value': float(num_matches[0].replace('$', '').replace(',', '')),
                            'unit': 'USD',  # Assuming dollars
                            'context': {'period': 'extracted', 'source': 'footnote'},
                            'concept_details': {'description': note.value[:200]}  # Truncate
                        }
                        self.fact_data.append(fact_entry)
                        fact_count += 1
                except:
                    continue
        
        # Method 2: Try parsing tables if available
        if fact_count == 0 and hasattr(self.modelXbrl, 'htmlDocs'):
            self.logger.info("Attempting to extract facts from HTML tables")
            # Implement table parsing logic here if needed
        
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
        
        if hasattr(concept, 'modelXbrl'):
            try:
                relset = concept.modelXbrl.relationshipSet("XBRL-parent-child")
                if relset is not None:
                    for rel in relset.fromModelObject(concept) or []:
                        if rel is not None and getattr(rel, 'toModelObject', None) is not None:
                            details['relationships']['parents'].append(
                                rel.toModelObject.qname.localName if hasattr(rel.toModelObject, 'qname') else None
                            )
                    for rel in relset.toModelObject(concept) or []:
                        if rel is not None and getattr(rel, 'fromModelObject', None) is not None:
                            details['relationships']['children'].append(
                                rel.fromModelObject.qname.localName if hasattr(rel.fromModelObject, 'qname') else None
                            )
            except Exception:
                pass
        
        details['relationships']['parents'] = [p for p in details['relationships']['parents'] if p is not None]
        details['relationships']['children'] = [c for c in details['relationships']['children'] if c is not None]
        
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
        xbrl_file = r"C:\Users\mithu\Documents\MEGA\Projects\Financial_Data_Analytics_Pipeline\filings\0000831259\10-Q\2014-03-31\fcx-20140331.xml"
        
        if processor.load_xbrl_file(xbrl_file):
            # Store company metadata and get company ID
            company_id = processor.store_company_metadata(xbrl_file)
            
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