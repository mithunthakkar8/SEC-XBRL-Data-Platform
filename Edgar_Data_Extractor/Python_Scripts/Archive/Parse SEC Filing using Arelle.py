from arelle import Cntlr
import yaml
from collections import defaultdict
import logging
import re
import os


class XBRLProcessor:
    def __init__(self, log_file='xbrl_processor.log'):
        """Initialize the XBRL processor with enhanced error handling"""
        self.cntlr = Cntlr.Cntlr(logFileName=log_file)
        self.modelXbrl = None
        self.fact_data = []
        self.grouped_facts = defaultdict(list)
        self.validation_errors = []
        
        # Configure logging
        self.logger = logging.getLogger("XBRLProcessor")
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler(log_file)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)
    
    def extract_company_metadata(self, xbrl_file):
        with open(xbrl_file, 'r', encoding='utf-8') as file:
            content = file.read()
        
        # Initialize variables
        extracted_data = {
            'form_type': None,
            'filed_as_of_date': None,
            'industry': None,  # Changed from 'sic'
            'sic': None,      # Will now contain only the number
            'accession_number': None,
            'company_name': None
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
            # Split into industry description and SIC code
            sic_parts = re.match(r'(.+?)\s*\[(\d+)\]', sic_full)
            if sic_parts:
                extracted_data['industry'] = sic_parts.group(1).strip().title()  # "METAL MINING" -> "Metal Mining"
                extracted_data['sic'] = sic_parts.group(2)  # Just the number "1000"
        
        # Extract Accession Number
        accession_match = re.search(r'ACCESSION NUMBER:\s+([^\n]+)', content)
        if accession_match:
            extracted_data['accession_number'] = accession_match.group(1).strip()
        
        # Extract and clean Company Name
        company_match = re.search(r'COMPANY CONFORMED NAME:\s+([^\n]+)', content)
        if company_match:
            # Remove non-alphanumeric characters (except spaces)
            cleaned_name = re.sub(r'[^a-zA-Z0-9\s]', '', company_match.group(1).strip())
            # Replace multiple spaces with single space
            cleaned_name = re.sub(r'\s+', ' ', cleaned_name)
            extracted_data['company_name'] = cleaned_name
        
        return extracted_data

    def store_company_metadata(self, xbrl_file, yaml_file):
        
        data = self.extract_company_metadata(xbrl_file)
        """Only updates YAML if file exists, otherwise does nothing"""
        if not os.path.exists(yaml_file):
            logging.error(f"No YAML file found at {yaml_file}. Data not written.")
            return None
        
        try:
            with open(yaml_file, 'r') as file:
                existing_data = yaml.safe_load(file) or []
            
            # Prepend new data
            if isinstance(existing_data, list):
                updated_data = [data] + existing_data
            else:
                updated_data = [data, existing_data]
            
            # Write back to YAML
            with open(yaml_file, 'w') as file:
                yaml.dump(updated_data, file, sort_keys=False, default_flow_style=False)
            
            logging.info(f"Successfully updated {yaml_file} with company metadata")
        except Exception as e:
            logging.error(f"Error updating YAML file: {e}")
            return None

    def _handle_validation_error(self, error_code, error_message, file_path):
        """Handle and log validation errors"""
        error_info = {
            'code': error_code,
            'message': error_message,
            'file': file_path
        }
        self.validation_errors.append(error_info)
        self.logger.warning(f"Validation Error [{error_code}]: {error_message} in {file_path}")
        
        # Special handling for common errors
        if "XML declaration allowed only at the start" in error_message:
            self.logger.info("This often occurs when there's embedded XML in document text")
        elif "Entity 'nbsp' not defined" in error_message:
            self.logger.info("This typically indicates HTML entities in the document")

    def load_xbrl_file(self, xbrl_file, validate=True, strict_validation=False):
        """Load an XBRL file with configurable validation"""
        try:
            self.cntlr.modelManager.validateInferXbrl = validate
            self.cntlr.modelManager.validateDisclosureSystem = strict_validation
            
            # Load with error callback
            self.modelXbrl = self.cntlr.modelManager.load(
                xbrl_file,
                errorHandler=self._handle_validation_error
            )
            
            if self.modelXbrl is None:
                self.logger.error(f"Failed to load XBRL file: {xbrl_file}")
                return False
                
            if self.validation_errors and strict_validation:
                self.logger.warning(f"Loaded with {len(self.validation_errors)} validation issues")
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"Critical error loading {xbrl_file}: {str(e)}")
            return False

    def _get_context_details(self, context):
        """Extract context details with error handling"""
        try:
            details = {}
            
            if context is not None:
                # Period information (with null checks)
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
                
                # Entity information
                try:
                    if context.entityIdentifier:
                        details['entity'] = context.entityIdentifier[1]
                except (AttributeError, IndexError):
                    pass
                
                # Scenario information
                try:
                    if context.scenario is not None:
                        scenario_qnames = [qname.localName for qname in context.scenario.qnameIter()]
                        if scenario_qnames:
                            details['scenario'] = scenario_qnames
                except AttributeError:
                    pass
                
                # Dimension extraction with robust error handling
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
        """Extract detailed information about a concept from the taxonomy (private method)"""
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
        
        # Standard label - using explicit existence check
        std_label = concept.label() if hasattr(concept, 'label') else None
        if std_label is not None:
            details['labels']['standard'] = std_label
        
        # Common XBRL label roles
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
        
        # Documentation - explicit None check
        if hasattr(concept, 'genDocs'):
            for doc in concept.genDocs:
                if hasattr(doc, 'value') and doc.value is not None:
                    cleaned = doc.value.strip()
                    if cleaned:  # Only add non-empty strings
                        details['documentation'].append(cleaned)
        
        # Relationships - explicit checks
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
        
        # Remove None values from relationships
        details['relationships']['parents'] = [p for p in details['relationships']['parents'] if p is not None]
        details['relationships']['children'] = [c for c in details['relationships']['children'] if c is not None]
        
        return details

    def load_xbrl_file(self, xbrl_file, validate=True):
        """Load and validate an XBRL file"""
        self.cntlr.modelManager.validateInferXbrl = validate
        self.modelXbrl = self.cntlr.modelManager.load(xbrl_file)
        return self.modelXbrl is not None

    def process_facts(self, skip_non_numeric=True):
        """Process facts with configurable numeric value checking"""
        if not self.modelXbrl:
            raise ValueError("No XBRL file loaded")
            
        self.fact_data = []
        self.grouped_facts = defaultdict(list)
        
        for fact in self.modelXbrl.facts:
            try:
                if fact.concept is None:
                    continue
                    
                concept_details = self._get_concept_details(fact.concept)
                
                # Value processing with configurable strictness
                try:
                    numeric_value = float(fact.value)
                except (ValueError, TypeError):
                    if skip_non_numeric:
                        continue
                    numeric_value = fact.value  # Keep original value
                
                context_details = self._get_context_details(fact.context)
                
                # Unit extraction with fallback
                unit = "unitless"
                try:
                    if fact.unit and fact.unit.measures:
                        unit = str(fact.unit.measures[0][0].localName)
                except Exception:
                    pass
                
                fact_entry = {
                    'concept': concept_details.get('name'),
                    'value': numeric_value,
                    'unit': unit,
                    'context': context_details,
                    'concept_details': {k: v for k, v in concept_details.items() if k != 'name'}
                }
                
                self.fact_data.append(fact_entry)
                self.grouped_facts[fact_entry['concept']].append({
                    'value': fact_entry['value'],
                    'unit': fact_entry['unit'],
                    'context': fact_entry['context'],
                    'concept_details': fact_entry['concept_details']
                })
                
            except Exception as e:
                self.logger.warning(f"Error processing fact: {str(e)}")
                continue
                
        return len(self.fact_data)

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

    def save_to_yaml(self, output_file):
        """Save processed data to a YAML file"""
        if not self.fact_data:
            raise ValueError("No facts processed. Call process_facts() first.")
        
        output_data = {
            'xbrl_facts': dict(self.grouped_facts),
            'metadata': {
                'source_file': self.modelXbrl.modelDocument.uri if self.modelXbrl.modelDocument else None,
                'facts_count': len(self.fact_data),
                'concepts_count': len(self.grouped_facts),
                'taxonomy': self.modelXbrl.modelDocument.uri if self.modelXbrl.modelDocument else None
            }
        }
        
        with open(output_file, 'w') as f:
            yaml.dump(output_data, f, sort_keys=False, default_flow_style=False, indent=2)
        
        return output_file

    def close(self):
        """Clean up resources"""
        if self.modelXbrl:
            self.cntlr.modelManager.close()
        self.modelXbrl = None
        self.fact_data = []
        self.grouped_facts = defaultdict(list)

    def __del__(self):
        """Destructor to ensure resources are cleaned up"""
        self.close()


# Example usage:
if __name__ == "__main__":
    processor = XBRLProcessor()
    
    try:
        # Load an XBRL file
        xbrl_file = r"C:\Users\mithu\Documents\MEGA\Projects\Financial_Data_Analytics_Pipeline\filings\0001001838\10-K\2019-12-31\0001558370-20-001781.txt"
        if processor.load_xbrl_file(xbrl_file):
            # Process facts
            fact_count = processor.process_facts()
            print(f"Processed {fact_count} facts")
            
            # Save to YAML
            output_file = 'xbrl_facts_output.yaml'
            saved_file = processor.save_to_yaml(output_file)
            print(f"Successfully wrote XBRL data to {saved_file}")

            processor.store_company_metadata(xbrl_file, output_file)
        else:
            print("Failed to load XBRL file")
    finally:
        processor.close()