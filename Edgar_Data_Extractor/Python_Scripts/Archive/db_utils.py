import logging
import sqlalchemy 
from sqlalchemy.exc import SQLAlchemyError
import json
from dotenv import load_dotenv
import os

# Initialize logger for this module
logger = logging.getLogger(__name__)

load_dotenv()

def create_db_engine():
    """Creates and returns a PostgreSQL database engine."""
    db_url = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    
    try:
        engine = sqlalchemy.create_engine(db_url)
        logger.info("✅ Database engine created successfully.")
        return engine

    except sqlalchemy.exc.SQLAlchemyError as db_error:
        logger.critical(f"❌ Failed to create database engine: {db_error}", exc_info=True)
        raise

def get_company_id(engine, cik):
    """Fetch company_id from the database using CIK."""
    query = sqlalchemy.text("SELECT company_id FROM alphascope.company WHERE cik = :cik")
    
    try:
        with engine.connect() as conn:
            # Execute the query to fetch company_id for the given CIK
            result = conn.execute(query, {"cik": cik}).fetchone()
        
        if result is None:
            logger.warning(f"⚠️ Company ID not found for CIK: {cik}")
            raise ValueError(f"Company ID not found for CIK: {cik}")

        logger.info(f"✅ Retrieved company_id {result[0]} for CIK: {cik}")
        return result[0]  # Extract company_id

    except sqlalchemy.exc.SQLAlchemyError as db_error:
        logger.error(f"❌ Database error while fetching company_id for CIK {cik}: {db_error}", exc_info=True)
        raise

def insert_company_metadata(engine, cik, name, ticker_symbol, sector, industry):
    """Inserts company metadata into the database."""
    query = sqlalchemy.text("""
        INSERT INTO alphascope.company (cik, name, ticker_symbol, sector, industry) 
        VALUES (:cik, :name, :ticker_symbol, :sector, :industry)
        ON CONFLICT (ticker_symbol) DO NOTHING
    """)

    try:
        with engine.connect() as conn:
            # Execute the insert query to add company metadata
            conn.execute(query, {
                "cik": cik,
                "name": name,
                "ticker_symbol": ticker_symbol,
                "sector": sector,
                "industry": industry
            })
            
            # Commit the transaction to save changes
            conn.commit()
        
        logger.info(f"✅ Inserted company metadata for {ticker_symbol} (CIK: {cik}).")

    except sqlalchemy.exc.SQLAlchemyError as db_error:
        logger.error(f"❌ Failed to insert company metadata for {ticker_symbol} (CIK: {cik}): {db_error}", exc_info=True)


def get_report_frequency(filing):
    """Determines the report type based on the filing form."""
    if "10-Q" in filing.form:
        return "Quarterly"
    elif "10-K" in filing.form:
        return "Annual"
    else:
        logger.error(f"❌ Invalid report type for filing form: {filing.form}")
        return None

def insert_filing(conn, filing):
    """Inserts filing data and returns the filing_id."""
    insert_filing_query = sqlalchemy.text("""
        INSERT INTO alphascope.filings (filing_date, filing_url)
        VALUES (:filing_date, :filing_url)
        ON CONFLICT (filing_date, filing_url) DO NOTHING
        RETURNING filing_id
    """)

    existing_filing_query = sqlalchemy.text("""
        SELECT filing_id FROM alphascope.filings
        WHERE filing_date = :filing_date AND filing_url = :filing_url
    """)

    try:
        trans = conn.begin()
        result = conn.execute(insert_filing_query, {
            "filing_date": filing.filing_date,
            "filing_url": filing.filing_url
        })
        filing_id = result.scalar()

        if filing_id is None:
            result = conn.execute(existing_filing_query, {
                "filing_date": filing.filing_date,
                "filing_url": filing.filing_url
            })
            filing_id = result.scalar()

        if filing_id:
            trans.commit()
            return filing_id
        else:
            trans.rollback()
            logger.error(f"❌ Filing insert failed, no filing_id found.")
            return None
    except Exception as e:
        trans.rollback()
        logger.error(f"❌ Failed to insert filing: {e}")
        return None

def insert_company_filing(conn, company_id, report_frequency, filing_id):
    """Inserts company filing data and returns the company_filing_id."""
    insert_company_filing_query = sqlalchemy.text("""
        INSERT INTO alphascope.company_filings (company_id, report_frequency, filing_id)
        VALUES (:company_id, :report_frequency, :filing_id)
        ON CONFLICT (company_id, report_frequency, filing_id) DO NOTHING
        RETURNING company_filing_id
    """)

    existing_query = sqlalchemy.text("""
        SELECT company_filing_id FROM alphascope.company_filings
        WHERE company_id = :company_id AND report_frequency = :report_frequency AND filing_id = :filing_id
    """)

    try:
        trans = conn.begin()
        result = conn.execute(insert_company_filing_query, {
            "company_id": company_id,
            "report_frequency": report_frequency,
            "filing_id": filing_id
        })
        company_filing_id = result.scalar()

        if company_filing_id is None:
            result = conn.execute(existing_query, {
                "company_id": company_id,
                "report_frequency": report_frequency,
                "filing_id": filing_id
            })
            company_filing_id = result.scalar()

        if company_filing_id:
            trans.commit()
            return company_filing_id
        else:
            trans.rollback()
            logger.error(f"❌ Company filing insert failed, no company_filing_id found.")
            return None
    except Exception as e:
        trans.rollback()
        logger.error(f"❌ Failed to insert company filing: {e}")
        return None

def insert_financial_metrics(conn, company_filing_id, metrics, filing):
    """Inserts financial metrics into the financial_metrics table."""
    insert_metric_query = sqlalchemy.text("""
        INSERT INTO alphascope.financial_metrics (company_filing_id, metric_type, period_of_report, value)
        VALUES (:company_filing_id, :metric_type, :period_of_report, :value)
        ON CONFLICT (company_filing_id, metric_type, period_of_report) DO NOTHING
    """)

    failed_inserts = []
    rows_inserted = 0

    for metric_type, value in metrics.items():
        try:
            with conn.begin():
                result = conn.execute(insert_metric_query, {
                    "company_filing_id": company_filing_id,
                    "metric_type": metric_type,
                    "period_of_report": filing.period_of_report,
                    "value": value[0]
                })
                rows_inserted += result.rowcount
        except sqlalchemy.exc.SQLAlchemyError as e:
            failed_inserts.append((metric_type, filing.period_of_report))
            logger.error(f"❌ Failed to insert {metric_type}: {e}")

    return rows_inserted, failed_inserts

def insert_test_reported_metrics(conn, metric_type, value, filing):
    """Inserts data into test_reported_metrics table."""
    insert_test_metric_query = sqlalchemy.text("""
        INSERT INTO alphascope.test_reported_metrics (metric_type, reported_metric, period_of_report)
        VALUES (:metric_type, :reported_metric, :period_of_report)
        ON CONFLICT (metric_type, reported_metric) DO NOTHING
    """)

    try:
        with conn.begin():
            result = conn.execute(insert_test_metric_query, {
                "metric_type": metric_type,
                "reported_metric": json.dumps(value[1].tolist()), 
                "period_of_report": filing.period_of_report
            })
            return result.rowcount
    except sqlalchemy.exc.SQLAlchemyError as e:
        logger.error(f"❌ Failed to insert test reported metric {metric_type}: {e}")
        return 0

def insert_financial_data(engine, company_id, filing, metrics):
    """Orchestrates the financial data insertion process."""
    report_frequency = get_report_frequency(filing)
    if report_frequency is None:
        return

    with engine.connect() as conn:
        filing_id = insert_filing(conn, filing)
        if filing_id is None:
            return

        company_filing_id = insert_company_filing(conn, company_id, report_frequency, filing_id)
        if company_filing_id is None:
            return

        rows_inserted, failed_inserts = insert_financial_metrics(conn, company_filing_id, metrics, filing)
        test_rows_inserted = 0
        for metric_type, value in metrics.items():
            test_rows_inserted += insert_test_reported_metrics(conn, metric_type, value, filing)

        # Log the number of successful inserts
        logger.info(f"✅ Inserted {rows_inserted} metrics for company_id {company_id}, period {filing.period_of_report} in the financial_metrics table.")
        logger.info(f"✅ Inserted {test_rows_inserted} metrics for period {filing.period_of_report} in the test_reported_metrics table.")

        if failed_inserts:
            logger.error(f"❌ Failed inserts for company_id {company_id}: {failed_inserts}")