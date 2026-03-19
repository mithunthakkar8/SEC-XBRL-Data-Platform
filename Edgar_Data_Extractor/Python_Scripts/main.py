from .XBRLProcessor import DataLoader, RelationshipLoader

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
    
    loader = DataLoader(db_config=db_config)
    
    
    try:
        # Load an XBRL file
        submission_file = r"C:\Users\mithu\Documents\MEGA\Projects\Financial_Data_Analytics_Pipeline\filings\0000831259\10-Q\2014-03-31\0000831259-14-000022.txt"
        instance_file = r"C:\Users\mithu\Documents\MEGA\Projects\Financial_Data_Analytics_Pipeline\filings\0000831259\10-Q\2014-03-31\fcx-20140331.xml"
        
        if loader.load_xbrl_file(instance_file):
            # Store company metadata and get company ID
            company_id = loader.load_metadata(submission_file)
            
            # Process facts with the company ID
            fact_count = loader.process_standard_facts(company_id=company_id)
            print(f"Processed {fact_count} facts")
            
            # Save to database
            loader.save_to_database(company_id)
            print("Successfully saved data to PostgreSQL database")
        else:
            print("Failed to load XBRL file")
    finally:
        loader.close()