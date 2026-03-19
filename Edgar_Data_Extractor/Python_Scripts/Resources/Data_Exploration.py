import pandas as pd  # Import pandas for data manipulation
import sys  # Import sys module to modify output buffering behavior
from edgar_utils import fetch_filings, extract_financial_metrics, process_filings  # Import utility functions for EDGAR data handling

# Set pandas display options to ensure full visibility of column contents
pd.set_option('display.max_colwidth', None)

# Ensure that output is not buffered, so it appears immediately in the console
sys.stdout.reconfigure(line_buffering=True)  

# Define the Central Index Key (CIK) for the company (unique identifier in SEC's EDGAR database)
CIK = '0001001838'  # Example CIK for a company

# Define the ticker symbol for the company (e.g., "SCCO" for Southern Copper Corporation)
TICKER_SYMBOL = "SCCO"

# Define the years for which filings are to be retrieved
YEARS = [2025]  # Example: Fetch filings for the year 2025

# Specify the type of SEC filings to fetch
FORMS = ["10-K"]  # Fetch annual reports (10-K); change to ["10-Q"] for quarterly reports

# Fetch the filings from the SEC EDGAR database for the specified CIK, years, and forms
filings = fetch_filings(CIK, YEARS, FORMS)

# Extract the label-to-concept mapping from the XBRL data of the first fetched filing
label_to_concept_map = filings[0].xbrl().label_to_concept_map

# Convert the label-to-concept mapping into a Pandas DataFrame for easy analysis
label_concept_df = pd.DataFrame(label_to_concept_map.items(), columns=['Label', 'Concept ID'])

# Save the label-to-concept mapping as a CSV file
label_concept_df.to_csv('label_to_concept_map.csv', index=False)

# Retrieve the names of the financial statements available in the filing
statement_names = filings[0].xbrl().statements.names

# Convert the list of statement names into a Pandas DataFrame
statements_df = pd.DataFrame(statement_names, columns=['Statement Name'])

# Save the list of available financial statements to a CSV file
statements_df.to_csv('list_of_statements.csv', index=False)

# Retrieve and display the financial data for a specific financial statement
# Example: 'DisclosureSegmentAndRelatedInformationSalesDetails' (Modify as needed)
filings[0].xbrl().get_statement('DisclosureSegmentAndRelatedInformationSalesDetails').get_dataframe()


# After viewing the result from the above statement, it is evident that segment or operational information is not easily 
# availed by the edgartools library. the library struggles with cash flow statement numbers too (as is seen on its github) 
# and is in development stage. Hence, moving onto other tasks for now. Will later return after completing other tasks. 