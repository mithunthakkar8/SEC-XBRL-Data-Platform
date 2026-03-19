from .db_utils import create_db_engine, insert_company_metadata
from .edgar_utils import fetch_filings, process_filings
from .yahoo_utils import fetch_company_metadata
import logging
import os
from pathlib import Path
import edgar
import yaml

def run_edgar_etl():
    # setup logging config
    logging.basicConfig (
    filename="Edgar_Data_Extractor_log.log", 
    filemode="a",        
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s"
    )

    # Get base path from environment variable
    base_path = Path(os.getenv("PROJECTS_PATH", "")) 

    yaml_file_path = base_path / "Financial_Data_Analytics_Pipeline\Query_Details.yaml"

    with open(yaml_file_path, "r") as file:
        config = yaml.safe_load(file)
    
    # Access values
    years = config.get("YEARS", [])
    forms = config.get("FORMS", [])
    email = config.get("email", "")
    metrics_path = config.get("metrics_path", "")
    ticker = config.get("ticker", "")
    
    # Define the types of filings to fetch (e.g., "10-K" for annual reports and "10-Q" for quarterly reports)
    # FORMS = ["10-K", "10-Q"]
    # FORMS = ["10-Q"]  # Alternative: Fetch only 10-Q filings

    # Create a database engine to connect to the database
    engine = create_db_engine()

    # Set identity for edgar package
    edgar.set_identity(email)

    company = edgar.Company(ticker)

    cik_num = company.cik
    cik = str(cik_num).zfill(10)

    # Fetch the filings for the specified CIK, years, and forms
    filings = fetch_filings(cik, years, forms)

    # Extract the company name from the first filing in the list
    company_name = filings[0].company
    
    # Fetch additional metadata (e.g., sector, industry) for the company using its ticker symbol
    metadata = fetch_company_metadata(ticker)
    
    # Insert the company metadata (CIK, name, ticker symbol, sector, industry) into the database
    insert_company_metadata(engine, cik, company_name, ticker, metadata["sector"], metadata["industry"])

    # Construct full path
    metrics_yaml_path = metrics_path

    # Process the filings to extract and store the specified metrics in the database
    process_filings(filings, engine, cik, metrics_yaml_path)

#-------------------------------------------------------------------------------------------------------------------------------
    # For Inserting metadata for other companies

#-------------------------------------------------------------------------------------------------------------------------------

    # FCX_CIK = '0000831259'
    # FCX_TICKER = "FCX"

    # HBM_CIK = '0001322422'
    # HBM_TICKER = "HBM"

    # ERO_CIK = '0001853860'
    # ERO_TICKER = "ERO"

    # MTAL_CIK = '0001950246'
    # MTAL_TICKER = "MTAL"

    # FORMS = ["10-K", "10-Q", "6-K", "10-F"]

    # FCX_filings = fetch_filings(FCX_CIK, YEARS, FORMS)
    # HBM_filings = fetch_filings(HBM_CIK, YEARS, FORMS)
    # ERO_filings = fetch_filings(ERO_CIK, YEARS, FORMS)
    # MTAL_filings = fetch_filings(MTAL_CIK, YEARS, FORMS)

    # # Extract the company name from the first filing in the list
    # FCX_company_name = FCX_filings[0].company

    # # Extract the company name from the first filing in the list
    # HBM_company_name = HBM_filings[0].company

    # # Extract the company name from the first filing in the list
    # ERO_company_name = ERO_filings[0].company

    # # Extract the company name from the first filing in the list
    # MTAL_company_name = MTAL_filings[0].company
    
    # FCX_metadata = fetch_company_metadata(FCX_TICKER)
    # HBM_metadata = fetch_company_metadata(HBM_TICKER)
    # ERO_metadata = fetch_company_metadata(ERO_TICKER)
    # MTAL_metadata = fetch_company_metadata(MTAL_TICKER)

    # insert_company_metadata(engine, FCX_CIK, FCX_company_name, FCX_TICKER, FCX_metadata["sector"], FCX_metadata["industry"])
    # insert_company_metadata(engine, HBM_CIK, HBM_company_name, HBM_TICKER, HBM_metadata["sector"], HBM_metadata["industry"])
    # insert_company_metadata(engine, ERO_CIK, ERO_company_name, ERO_TICKER, ERO_metadata["sector"], ERO_metadata["industry"])
    # insert_company_metadata(engine, MTAL_CIK, MTAL_company_name, MTAL_TICKER, MTAL_metadata["sector"], MTAL_metadata["industry"])
