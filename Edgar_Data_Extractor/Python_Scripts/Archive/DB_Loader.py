import yaml
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
from datetime import datetime
import uuid
import requests
from typing import Dict, List, Any, Optional
from yahooquery import Ticker
import time

class XbrlYamlToPostgresLoader:
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the loader with database configuration.
        
        Args:
            db_config: Dictionary with PostgreSQL connection parameters
                       (host, database, user, password, port)
        """
        self.db_config = db_config
        self.conn = None
        
    def _get_ticker_from_cik(self, cik):
        cik = str(cik).zfill(10)  # Ensure CIK is 10 digits
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        headers = {'User-Agent': 'Your Company Name your@email.com'}
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            return data.get('tickers', [None])[0]
        except Exception as e:
            print(f"Error fetching data for CIK {cik}: {e}")
            return None

    def _get_yahoo_industry_data(self, ticker_symbol: str) -> Dict[str, Optional[str]]:
        """
        Fetch industry and sector data from Yahoo Finance.
        
        Args:
            ticker_symbol: Stock ticker symbol
            
        Returns:
            Dictionary with yahoo_industry and yahoo_sector
        """
        try:
            ticker = Ticker(ticker_symbol)
            summary = ticker.summary_profile
            if not summary or ticker_symbol not in summary:
                return {'yahoo_industry': None, 'yahoo_sector': None}
            
            profile = summary[ticker_symbol]
            return {
                'yahoo_industry': profile.get('industry'),
                'yahoo_sector': profile.get('sector')
            }
        except Exception as e:
            print(f"Error fetching Yahoo data for {ticker_symbol}: {e}")
            return {'yahoo_industry': None, 'yahoo_sector': None}

    def _load_company_data(self, yaml_data: Dict[str, Any]) -> str:
        """
        Load company data into the database.
        
        Args:
            yaml_data: Parsed YAML data
            
        Returns:
            company_id from the database
        """
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        with self.conn.cursor() as cursor:
            # Extract company data from YAML
            company_name = yaml_data['company_name']
            cik = yaml_data.get('cik', '0000000000')
            sic = yaml_data.get('sic', '0000')
            
            # Get ticker symbol from CIK
            ticker_symbol = self._get_ticker_from_cik(cik)
            if not ticker_symbol:
                ticker_symbol = company_name.split()[0].upper()  # Fallback
            
            # Get Yahoo industry data
            yahoo_data = self._get_yahoo_industry_data(ticker_symbol)
            
            # Insert into company table
            company_insert = """
            INSERT INTO xbrl.company (cik, name, ticker_symbol, sic)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ticker_symbol) DO UPDATE
            SET name = EXCLUDED.name, cik = EXCLUDED.cik, sic = EXCLUDED.sic
            RETURNING company_id;
            """
            cursor.execute(company_insert, (cik, company_name, ticker_symbol, sic))
            company_id = cursor.fetchone()[0]
            
            # Insert into industry table
            industry_insert = """
            INSERT INTO xbrl.industry (ticker_symbol, sec_industry, yahoo_industry, yahoo_sector)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ticker_symbol) DO UPDATE
            SET sec_industry = EXCLUDED.sec_industry,
                yahoo_industry = EXCLUDED.yahoo_industry,
                yahoo_sector = EXCLUDED.yahoo_sector;
            """
            cursor.execute(industry_insert, (
                ticker_symbol,
                yaml_data.get('industry'),
                yahoo_data['yahoo_industry'],
                yahoo_data['yahoo_sector']
            ))
            
            return company_id

    def _load_filing_data(self, yaml_data: Dict[str, Any], company_id: str) -> str:
        """
        Load filing metadata into the database.
        
        Args:
            yaml_data: Parsed YAML data
            company_id: UUID from company table
            
        Returns:
            filing_id from the database
        """
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        with self.conn.cursor() as cursor:
            # Extract filing data from YAML
            accession_number = yaml_data['accession_number']
            filing_date = datetime.strptime(yaml_data['filed_as_of_date'], '%Y-%m-%d').date()
            filing_type = yaml_data['form_type']
            
            # For period_end, use the latest end date from the facts
            period_end = None
            for fact_list in yaml_data.get('xbrl_facts', {}).values():
                for fact in fact_list:
                    if 'period' in fact['context'] and fact['context']['period']['end']:
                        end_date = datetime.strptime(fact['context']['period']['end'], '%Y-%m-%d').date()
                        if period_end is None or end_date > period_end:
                            period_end = end_date
            
            if period_end is None:
                period_end = filing_date  # Fallback
            
            # Insert into filing table
            filing_insert = """
            INSERT INTO xbrl.filing (filing_id, company_id, accession_number, filing_date, filing_type, period_end)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (accession_number) DO UPDATE
            SET filing_date = EXCLUDED.filing_date,
                filing_type = EXCLUDED.filing_type,
                period_end = EXCLUDED.period_end
            RETURNING filing_id;
            """
            filing_id = str(uuid.uuid4())
            cursor.execute(filing_insert, (
                filing_id,
                company_id,
                accession_number,
                filing_date,
                filing_type,
                period_end
            ))
            
            return filing_id

    def _load_contexts(self, filing_id: str, yaml_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Load contexts from YAML data.
        
        Args:
            filing_id: UUID from filing table
            yaml_data: Parsed YAML data
            
        Returns:
            Dictionary mapping context hashes to context_ids
        """
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        context_map = {}
        
        with self.conn.cursor() as cursor:
            seen_contexts = set()
            
            for fact_list in yaml_data.get('xbrl_facts', {}).values():
                for fact in fact_list:
                    context = fact['context']
                    entity = context['entity']
                    
                    # Handle period data
                    period = context.get('period', {})
                    period_start = datetime.strptime(period['start'], '%Y-%m-%d').date() if 'start' in period else None
                    period_end = datetime.strptime(period['end'], '%Y-%m-%d').date() if 'end' in period else None
                    instant = datetime.strptime(context['instant'], '%Y-%m-%d').date() if 'instant' in context else None
                    
                    # Handle dimensions
                    dimensions = context.get('dimensions', {})
                    segment = Json(dimensions) if dimensions else None
                    
                    # Create unique context hash
                    context_hash = f"{filing_id}-{entity}-{period_start}-{period_end}-{instant}-{segment}"
                    
                    if context_hash not in seen_contexts:
                        seen_contexts.add(context_hash)
                        
                        # Insert context
                        context_insert = """
                        INSERT INTO xbrl.context (context_id, filing_id, entity, period_start, period_end, instant, segment)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (filing_id, entity, period_start, period_end, segment) DO UPDATE
                        SET instant = EXCLUDED.instant
                        RETURNING context_id;
                        """
                        context_id = str(uuid.uuid4())
                        cursor.execute(context_insert, (
                            context_id,
                            filing_id,
                            entity,
                            period_start,
                            period_end,
                            instant,
                            segment
                        ))
                        
                        context_map[context_hash] = context_id
                        
                        # Insert dimensions if they exist
                        if dimensions:
                            for dim_name, dim_value in dimensions.items():
                                dim_insert = """
                                INSERT INTO xbrl.dimension (dimension_id, context_id, dimension_name, member_name)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT DO NOTHING;
                                """
                                cursor.execute(dim_insert, (
                                    str(uuid.uuid4()),
                                    context_id,
                                    dim_name,
                                    dim_value
                                ))
        
        return context_map

    def _load_concepts_and_facts(self, yaml_data: Dict[str, Any], context_map: Dict[str, str]):
        """
        Load concepts and facts from YAML data.
        
        Args:
            yaml_data: Parsed YAML data
            context_map: Dictionary mapping context hashes to context_ids
        """
        if not self.conn:
            raise RuntimeError("Database connection not established")
            
        with self.conn.cursor() as cursor:
            for concept_name, fact_list in yaml_data.get('xbrl_facts', {}).items():
                for fact in fact_list:
                    context = fact['context']
                    entity = context['entity']
                    
                    # Handle period data
                    period = context.get('period', {})
                    period_start = datetime.strptime(period['start'], '%Y-%m-%d').date() if 'start' in period else None
                    period_end = datetime.strptime(period['end'], '%Y-%m-%d').date() if 'end' in period else None
                    instant = datetime.strptime(context['instant'], '%Y-%m-%d').date() if 'instant' in context else None
                    
                    # Handle dimensions
                    dimensions = context.get('dimensions', {})
                    segment = Json(dimensions) if dimensions else None
                    
                    # Get context_id
                    context_hash = f"{yaml_data['filing_id']}-{entity}-{period_start}-{period_end}-{instant}-{segment}"
                    context_id = context_map.get(context_hash)
                    
                    if not context_id:
                        continue
                    
                    # Get concept details
                    concept_details = fact.get('concept_details', {})
                    labels = concept_details.get('labels', {})
                    attributes = concept_details.get('attributes', {})
                    
                    # Insert label
                    label_insert = """
                    INSERT INTO xbrl.label (concept_name, standard_label, namespace)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (concept_name) DO UPDATE
                    SET standard_label = EXCLUDED.standard_label,
                        namespace = EXCLUDED.namespace;
                    """
                    cursor.execute(label_insert, (
                        concept_name,
                        labels.get('standard', ''),
                        concept_details.get('namespace', '')
                    ))
                    
                    # Insert concept attributes
                    attr_insert = """
                    INSERT INTO xbrl.concept_attribute (
                        concept_name, namespace, period_type, data_type, is_abstract, is_nillable
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (concept_name) DO UPDATE
                    SET namespace = EXCLUDED.namespace,
                        period_type = EXCLUDED.period_type,
                        data_type = EXCLUDED.data_type,
                        is_abstract = EXCLUDED.is_abstract,
                        is_nillable = EXCLUDED.is_nillable;
                    """
                    cursor.execute(attr_insert, (
                        concept_name,
                        concept_details.get('namespace', ''),
                        attributes.get('period_type', 'duration'),
                        attributes.get('data_type', ''),
                        attributes.get('is_abstract', False),
                        attributes.get('is_nillable', True)
                    ))
                    
                    # Insert the fact
                    fact_insert = """
                    INSERT INTO xbrl.reported_fact (
                        fact_id, context_id, concept_name, numeric_value, string_value, 
                        boolean_value, date_value, unit, balance_type
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (context_id, concept_name) DO UPDATE
                    SET numeric_value = EXCLUDED.numeric_value,
                        string_value = EXCLUDED.string_value,
                        boolean_value = EXCLUDED.boolean_value,
                        date_value = EXCLUDED.date_value,
                        unit = EXCLUDED.unit,
                        balance_type = EXCLUDED.balance_type;
                    """
                    
                    numeric_value = fact.get('value')
                    string_value = fact.get('string_value')
                    boolean_value = fact.get('boolean_value')
                    date_value = datetime.strptime(fact['date_value'], '%Y-%m-%d').date() if 'date_value' in fact else None
                    
                    cursor.execute(fact_insert, (
                        str(uuid.uuid4()),
                        context_id,
                        concept_name,
                        numeric_value,
                        string_value,
                        boolean_value,
                        date_value,
                        fact.get('unit'),
                        attributes.get('balance')
                    ))

    def load_yaml_file(self, yaml_file_path: str):
        """
        Main method to load a YAML file into PostgreSQL.
        
        Args:
            yaml_file_path: Path to the YAML file containing XBRL data
        """
        # Load YAML data
        with open(yaml_file_path, 'r') as file:
            yaml_data = yaml.safe_load(file)
        
        # Connect to PostgreSQL
        self.conn = psycopg2.connect(**self.db_config)
        
        try:
            # Start transaction
            self.conn.autocommit = False
            
            # Load company data
            company_id = self._load_company_data(yaml_data)
            
            # Load filing data
            filing_id = self._load_filing_data(yaml_data, company_id)
            
            # Add filing_id to YAML data for context mapping
            yaml_data['filing_id'] = filing_id
            
            # Load contexts
            context_map = self._load_contexts(filing_id, yaml_data)
            
            # Load concepts and facts
            self._load_concepts_and_facts(yaml_data, context_map)
            
            # Commit transaction
            self.conn.commit()
            print(f"Successfully loaded data from {yaml_file_path}")
            
        except Exception as e:
            self.conn.rollback()
            print(f"Error loading data: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()
                self.conn = None

# Example usage
if __name__ == "__main__":
    # Database configuration
    db_config = {
        'host': 'localhost',
        'database': 'finhub',
        'user': 'finhub_admin',
        'password': 'pass@123',
        'port': '5432'
    }
    
    # Create loader instance
    loader = XbrlYamlToPostgresLoader(db_config)
    
    # Load a YAML file
    yaml_file_path = r"C:\Users\mithu\Documents\MEGA\Projects\Financial_Data_Analytics_Pipeline\xbrl_facts_output.yaml"
    loader.load_yaml_file(yaml_file_path)