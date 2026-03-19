from lxml import etree
import os
from datetime import datetime
import yaml



class XBRLInstanceParser:
    def __init__(self, xbrl_file):
        """
        Initialize the XBRL Instance parser with either:
        - Path to XBRL file (for automatic namespace detection), or
        - Direct financial concept namespace mapping
        
        Args:
            xbrl_file: Path to XBRL file
            financial_concept_namespace: Direct {prefix: uri} mapping for financial concepts
        """
        if not os.path.exists(xbrl_file):
            raise FileNotFoundError(f"XBRL file not found: {xbrl_file}")
        self.xbrl_file = xbrl_file
        tree = etree.parse(self.xbrl_file)
        self.root = tree.getroot()
        self.financial_concept_namespace = self._extract_financial_concept_namespace(self)
    
    @staticmethod
    def _extract_financial_concept_namespace(self):
        """
        Extract the primary financial concept namespace from the root element.
        Returns a dictionary with {prefix: uri} or empty dict if none found.
        
        Priority:
        1. US GAAP (modern FASB -> legacy XBRL US)
        2. IFRS/IND-AS
        3. First company-specific extension
        4. First non-core namespace as fallback
        """

        """Load and parse the XBRL file."""

        nsmap = self.root.nsmap
        priority_checks = [
            # 1. Modern US-GAAP (FASB-hosted)
            lambda prefix, uri: 'fasb.org/us-gaap/' in uri.lower(),
            
            # 2. Legacy US-GAAP (XBRL US-hosted)
            lambda prefix, uri: 'xbrl.us/us-gaap/' in uri.lower(),
            
            # 3. IFRS Global Standards
            lambda prefix, uri: 'ifrs.org/taxonomy' in uri.lower(),
            
            # 4. Indian Accounting Standards
            lambda prefix, uri: 'xbrl.ind-as.org' in uri.lower(),
            
            # 5. Company-specific Extensions
            lambda prefix, uri: any(
                x in uri.lower() 
                for x in ['http://www.', '.com/', '.inc/']
            ),
            
            # 6. Final Fallback (any non-core namespace)
            lambda prefix, uri: prefix not in ['xbrli', 'link', 'xlink', 'xsi']
        ]

        # Process all namespaces using priority checks
        for check in priority_checks:               # Loop through checks in priority order
            for prefix, uri in nsmap.items():      # Examine each namespace in the document
                # Normalize URI (handle None values and make case-insensitive)
                normalized_uri = uri.lower() if uri else ''
                
                # If current check passes for this namespace
                if check(prefix, normalized_uri):
                    # Return the first matching namespace found
                    return {prefix:uri}
        
        return {}  # No concept namespace found
    

    def process_facts(self, facts, target_period, reporting_frequency):
        """
        Extract and filter facts by detecting period type using startswith() patterns.
        
        Args:
            facts: List of XBRL facts (lxml elements)
            target_period: End date in MM-DD-YYYY format
            reporting_frequency: "Quarterly", "Yearly", or "Semi-Annual"
        
        Returns:
            List of tuples: (value, context_ref, start_date, end_date, period_type)
        """
        fact_data = []
        target_period_dt = datetime.strptime(target_period, "%m-%d-%Y")
        
        for fact in facts:
            context_ref = fact.attrib.get("contextRef", "")
            value = fact.text.strip() if fact.text else None

            # Handle INSTANT facts (AsOf_MM_DD_YYYY)
            if context_ref.startswith("As_Of_"):
                instant_date = context_ref[6:16].replace("_", "-").rstrip("-")  # Extract MM-DD-YYYY
                try:
                    instant_dt = datetime.strptime(instant_date, "%m-%d-%Y")
                    if instant_dt == target_period_dt:
                        fact_data.append((value, context_ref, None, instant_date, "Instant"))
                except ValueError as ve:
                    print('Warning:', ve)
                    continue
                continue  # Skip duration checks for instant facts

            # Handle DURATION facts (Duration_MM_DD_YYYY_To_MM_DD_YYYY)
            if context_ref.startswith("Duration_"):
                date_part = context_ref[9:]  # Remove "Duration_"
                date_range = date_part.split("_To_")
                if len(date_range) == 2:
                    start_date = date_range[0].replace("_", "-")
                    end_date = date_range[1][:10].replace("_", "-").rstrip("-")
                    
                    try:
                        start_dt = datetime.strptime(start_date, "%m-%d-%Y")
                        end_dt = datetime.strptime(end_date, "%m-%d-%Y")
                        
                        # Verify the end date matches target period
                        if end_dt != target_period_dt:
                            continue
                            
                        # Classify reporting frequency
                        duration_days = (end_dt - start_dt).days
                        if 85 <= duration_days <= 100:
                            period_type = "Quarterly"
                        elif 180 <= duration_days <= 190:
                            period_type = "Semi-Annual"
                        elif 360 <= duration_days <= 370:
                            period_type = "Annual"
                        else:
                            continue  # Skip unexpected durations
                        
                        if period_type == reporting_frequency:
                            fact_data.append((value, context_ref, start_date, end_date, period_type))
                            
                    except ValueError as ve:
                        print('Warning:', ve)
                        continue

                    except Exception as e:
                        print(f"Exception: {e}")

        return fact_data

    def load_metrics_config(self, yaml_path):
        """
        Load and validate the metrics configuration from YAML file.
        
        Args:
            yaml_path: Path to YAML configuration file
        
        Returns:
            Dictionary containing both metric_patterns and fallback_patterns
        """
        with open(yaml_path) as f:
            config = yaml.safe_load(f)
        
        # Validate required sections exist
        if 'metric_patterns' not in config or 'fallback_patterns' not in config:
            raise ValueError("YAML config must contain both 'metric_patterns' and 'fallback_patterns' sections")
        
        return config

    def extract_financials(self, target_period, reporting_frequency, metric_config):
        """
        Extract financial metrics with automatic fallback to secondary patterns.
        
        Args:
            root: XML root element
            target_period: End date in MM-DD-YYYY format
            reporting_frequency: "Quarterly", "Yearly", or "Semi-Annual"
            yaml_path: Path to YAML configuration file
        
        Returns:
            Dictionary of {display_name: (value, context_ref, start_date, end_date, period_type)}
        """
        # Load configuration
        config = self.load_metrics_config(metric_config)
        final_results = {}
        
        for metric_name, metric_data in config['metric_patterns'].items():
            display_name = metric_name.replace('_', ' ')
            primary_patterns = metric_data['patterns']
            fallback_patterns = config['fallback_patterns'].get(metric_name, [])
            
            # Search for primary patterns
            facts = []
            for pattern in primary_patterns:
                matched = self.root.xpath(f"//*[local-name()='{pattern}']", namespaces=self.financial_concept_namespace)
                facts.extend(matched)
            
            # If no matches found, try fallback patterns
            if not facts and fallback_patterns:
                for pattern in fallback_patterns:
                    matched = self.root.xpath(f"//*[local-name()='{pattern}']", namespaces=self.financial_concept_namespace)
                    facts.extend(matched)
            
            # Process whatever facts we found (primary or fallback)
            processed_facts = self.process_facts(facts, target_period, reporting_frequency)
            
            # Sort by context simplicity and magnitude
            processed_facts.sort(key=lambda x: (
                len(x[1]),
                -abs(float(x[0])) if x[0] and x[0].replace('.','',1).isdigit() else 0
            ))
            
            final_results[display_name] = processed_facts[0] if processed_facts else (None, None, None, None, None)
        
        return final_results



class XBRL_Metadata_Parser:
    def __init__(self, xbrl_file=None, xml_content=None):
        """
        Initialize parser with either:
        - Path to XBRL file (xbrl_file), or
        - Direct XML content (xml_content)
        """
        if xbrl_file:
            with open(xbrl_file, 'rb') as f:
                self.xml_content = f.read()
        else:
            raise ValueError("Must provide either xbrl_file or xml_content")
        
        self.root, self.nsmap = self._extract_namespaces()
        self.dei_prefix = self._detect_dei_prefix()

    def _extract_namespaces(self):
        """Parse XML and return root element with normalized namespace map"""
        root = etree.fromstring(self.xml_content)
        nsmap = root.nsmap.copy()
        
        # Handle default namespace if present
        if None in nsmap:
            nsmap['defaultns'] = nsmap.pop(None)
        
        return root, nsmap

    def _detect_dei_prefix(self):
        """Detect DEI prefix from namespace map"""
        dei_prefix = next(
            (prefix for prefix, uri in self.nsmap.items() 
             if uri and uri.startswith('http://xbrl.sec.gov/dei/')),
            None
        )
        if not dei_prefix:
            raise ValueError("No DEI namespace found. Available namespaces: " + str(self.nsmap))
        return dei_prefix

    def extract_SEC_dei_info(self):
        """
        Extract SEC DEI information from parsed document
        Returns dict with:
        - fiscal_year_end
        - fiscal_period
        - fiscal_year
        - period_end
        - document_type
        """
        queries = {
            'fiscal_year_end': f'//{self.dei_prefix}:CurrentFiscalYearEndDate/text()',
            'fiscal_period': f'//{self.dei_prefix}:DocumentFiscalPeriodFocus/text()',
            'fiscal_year': f'//{self.dei_prefix}:DocumentFiscalYearFocus/text()',
            'period_end': f'//{self.dei_prefix}:DocumentPeriodEndDate/text()',
            'document_type': f'//{self.dei_prefix}:DocumentType/text()'
        }
        
        return {
            key: self.root.xpath(query, namespaces=self.nsmap)[0] 
            if self.root.xpath(query, namespaces=self.nsmap) 
            else None
            for key, query in queries.items()
        }

    
xbrl_instance_file = r"C:\Users\mithu\Documents\MEGA\Projects\Financial_Data_Analytics_Pipeline\downloads\2020-12-31\instance.xml"
metrics_config = r"C:\Users\mithu\Documents\MEGA\Projects\Financial_Data_Analytics_Pipeline\Edgar_Data_Extractor\Python_Scripts\Resources\metrics_config_SCCO.yaml"

metadata_parser = XBRL_Metadata_Parser(xbrl_instance_file)

dei_metadata = metadata_parser.extract_SEC_dei_info()
fiscal_period = dei_metadata['fiscal_period']  # Example: "Q2"

if fiscal_period.startswith("Q"):  
    reporting_frequency = "Quarterly"
elif fiscal_period == "FY":
    reporting_frequency = "Annual"
elif fiscal_period in ("H1", "H2"):
    reporting_frequency = "Semi-Annual"
else:
    reporting_frequency = "Unknown"


target_period = datetime.strptime(dei_metadata['period_end'], "%Y-%m-%d").strftime("%m-%d-%Y")


# Initialize parser with namespace
parser = XBRLInstanceParser(xbrl_instance_file)

# Parse an XBRL document
results = parser.extract_financials(
    target_period=target_period,
    reporting_frequency=reporting_frequency,
    metric_config=metrics_config
)

# Print results
for item, data in results.items():
    if data:
        print(f"{item}: {data[0]}, Period: {data[2]} to {data[3]} ({data[4]})")
    else:
        print(f"No relevant {reporting_frequency.lower()} {item} found for {target_period}.")








