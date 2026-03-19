import logging
import yaml
import edgar
from . import db_utils 
import importlib
import pandas as pd

importlib.reload(db_utils)

# Initialize logger
logger = logging.getLogger(__name__)


# Reduce third-party logging noise
logging.getLogger("edgar").setLevel(logging.ERROR)
logging.getLogger("sqlalchemy").setLevel(logging.WARNING)


def fetch_filings(cik, years, forms):
    """Fetches SEC filings for the given CIK, years, and forms."""
    # Log the start of the process with the provided CIK, years, and forms
    logger.info(f"Fetching SEC filings for CIK: {cik}, Years: {years}, Forms: {forms}")
    
    # Use the `edgar` library to fetch filings for the specified years, forms, and CIK
    filings = edgar.get_filings(year=years, form=forms, amendments=True).filter(cik=cik)
    
    # Check if no filings were found
    if not filings:
        # Log a warning and raise an error if no filings are found
        logger.warning(f"No filings found for CIK: {cik}")
        raise ValueError(f"No filings found for CIK: {cik}")
    
    # Log the number of filings fetched
    logger.info(f"Fetched {len(filings)} filings for CIK: {cik}")
    
    # Return the fetched filings
    return filings

# Function to Load Metrics from YAML File
def load_metric_patterns(yaml_file_path):
    """Load metric patterns and fallback patterns from a YAML file."""
    with open(yaml_file_path, "r") as file:
        config = yaml.safe_load(file)
    return config["metric_patterns"], config["fallback_patterns"]

def extract_financial_metrics(financial_report, filing_type, period_of_report, yaml_file_path):
    """Extract key financial metrics from the given financial report using patterns from a YAML file."""
    try:
        # Load metric patterns and fallback patterns from YAML
        metric_patterns, fallback_patterns = load_metric_patterns(yaml_file_path)
        
        # Extract financial statements as DataFrames
        financial_statements = {
            "income_statement": financial_report.get_income_statement().get_dataframe(),
            "balance_sheet": financial_report.get_balance_sheet().get_dataframe(),
            "cash_flow": financial_report.get_cash_flow_statement().get_dataframe()
        }

        # Get the first column name dynamically
        first_col = financial_statements["income_statement"].columns[0]  

        # Build metric configurations dynamically
        metric_configurations = {}
        for metric_name, metric_data in metric_patterns.items():
            source = metric_data["source"]
            patterns = metric_data["patterns"]
            metric_configurations[metric_name] = {
                "pattern": patterns,
                "fallback": fallback_patterns.get(metric_name, []),
                "source": financial_statements[source]
            }

        # Extract metrics
        def extract_metrics(metric_configurations, first_col, column="concept"):
            metrics = {}
            for metric_name, config in metric_configurations.items():
                metrics[metric_name] = extract_metric(
                    config["source"],  
                    config["pattern"],
                    first_col,
                    column=column,
                    fallback_pattern=config["fallback"]
                )
            return metrics

        # Extract all metrics
        metrics = extract_metrics(metric_configurations, first_col)

        # Log extracted metrics
        logger.info(f"Extracted metrics for {filing_type}, {period_of_report}: {metrics}")
        
        return metrics
    
    except (AttributeError, FileNotFoundError, KeyError) as e:
        logger.warning(f"Error processing financial data for {period_of_report}, filing type {filing_type}: {str(e)}")
        return None


def extract_metric(df, patterns, column_name, column="index", fallback_pattern=None, fallback_column=None):
    """
    Extracts a financial metric based on patterns, with an optional fallback.

    If no match is found using patterns, fallback_pattern is tried in fallback_column.

    Args:
        df (pd.DataFrame): DataFrame containing financial data.
        patterns (str | list): Primary search pattern(s).
        column_name (str): Column to extract the metric from.
        column (str, optional): Column to search in; defaults to index.
        fallback_pattern (str | list, optional): Secondary pattern if primary fails.
        fallback_column (str, optional): Column to search for fallback_pattern; defaults to column.

    Returns:
        First matching value or None.
    """
    try:
        def extract_from_column(patterns, search_col):
            """Extract values based on pattern matching in the specified column or index."""
            # Create a mask to filter rows where the search column matches the pattern(s)
            mask = df[search_col].isin(patterns)  
            # Return the matching values and their indices
            return df.loc[mask, column_name], df.index[mask]

        # Try to extract the metric using the primary patterns
        metric_value, metric_reported = extract_from_column(patterns, column)
        
        # If no match is found and a fallback pattern is provided, try the fallback
        if metric_value.empty and fallback_pattern:
            metric_value, metric_reported = extract_from_column(fallback_pattern, fallback_column or column)

        # Return the first matching value (or None if no match is found)
        return metric_value.iloc[0] if not metric_value.empty else None, metric_reported

    except Exception as e:
        # Log an error if extraction fails
        logger.error(f"Error extracting metric: {e}")
        return None
    

def process_filings(filings, engine, cik, metrics_yaml_path):
    """Processes SEC filings, extracts financial data, and inserts it into the database."""
    try:
        # Get the company ID from the database using the CIK
        company_id = db_utils.get_company_id(engine, cik)
        
        # Log the start of processing for the company
        logger.info(f"Processing filings for company ID {company_id}, CIK: {cik}")
        
        # Iterate through each filing
        for filing in filings:
            try:
                # Extract the financial report from the filing
                financial_report = filing.obj().financials
                
                # Extract financial metrics from the report
                metrics = extract_financial_metrics(financial_report, filing.form, filing.period_of_report, metrics_yaml_path)

                # If metrics are successfully extracted, insert them into the database
                if metrics:
                    db_utils.insert_financial_data(engine, company_id, filing, metrics)
            except Exception as e:
                # Log a warning if processing a filing fails
                logger.warning(f"Failed to process filing {filing.period_of_report}: {e}")
    except Exception as e:
        # Log an error if processing filings for the CIK fails
        logger.error(f"Error processing filings for CIK {cik}: {e}")