# SEC XBRL Data Warehousing Pipeline

[![DOI](https://zenodo.org/badge/DOI/YOUR-DOI-HERE.svg)](https://doi.org/YOUR-DOI-HERE)

## 📌 Overview

This project implements an **end-to-end ETL pipeline** for extracting, transforming, and loading SEC XBRL financial filings into a PostgreSQL data warehouse. It includes automated scraping of SEC EDGAR, XBRL parsing using the Arelle toolkit, enrichment with external data sources (LEI, Yahoo Finance, SEC API), and analytical views for financial statement generation.

## 🚀 Key Features

| Feature | Description |
|---------|-------------|
| **Automated SEC Scraping** | Rate-limited scraping of 10-K, 10-Q filings with retry logic and jitter |
| **XBRL Parsing** | Full taxonomy loading, fact extraction, context preservation, relationship mapping |
| **Data Enrichment** | LEI lookup (GLEIF API), ticker resolution (SEC API), company metadata (Yahoo Finance) |
| **PostgreSQL Data Warehouse** | Normalized schema with industry classifications, company metadata, filings, contexts, facts, and concept relationships |
| **Financial Statement Views** | Dynamic pivot views for Income Statement, Balance Sheet, and Cash Flow Statement |
| **Segment/Scenario Support** | Handles dimensional contexts for segment reporting |

## 🛠️ Tech Stack

| Category | Technologies |
|----------|--------------|
| **Languages** | Python 3, PL/pgSQL, SQL |
| **XBRL Processing** | Arelle toolkit |
| **Web Scraping** | httpx, lxml, requests |
| **Database** | PostgreSQL (uuid-ossp, pg_stat_activity) |
| **External APIs** | GLEIF (LEI), SEC EDGAR, Yahoo Finance (yahooquery) |
| **Vector Embeddings** | Sentence Transformers (all-MiniLM-L6-v2) |

## 📊 Data Warehouse Schema

The pipeline populates the following schema structure:
xbrl.industry_classification → GICS/SIC/NAICS industry hierarchy
xbrl.company → Core company metadata (LEI, ticker, country)
xbrl.filing → SEC filing metadata (accession number, dates)
xbrl.context_period → Duration or instant contexts
xbrl.context → Contexts with period references
xbrl.dimension_declaration → Segment/scenario dimensions
xbrl.dimension_member → Dimension members
xbrl.concept → XBRL concepts from taxonomy
xbrl.label → Standard/verbose labels, documentation
xbrl.concept_attribute → Period type, balance type, data type
xbrl.reported_fact → Fact values (numeric, string, boolean, date)
xbrl.concept_relationship → Parent-child relationships (presentation)



## 🔄 Pipeline Architecture
SEC EDGAR
│
▼
SECScraper.py (rate-limited HTTP)
│
▼
Raw filings (.txt submission + .xml instance)
│
▼
XBRLToPostgresLoader.py (Arelle + enrichment)
│
├── load_metadata() → xbrl.company, xbrl.filing
├── load_xbrl_file() → XBRL model loading
├── load_concepts_batch() → xbrl.concept, label, attribute
├── load_contexts_batch() → xbrl.context, context_period, dimensions
├── _process_standard_facts() → xbrl.reported_fact
└── process_xbrl_relationships() → xbrl.concept_relationship
│
▼
PostgreSQL Data Warehouse
│
▼
Financial Statement Views (SQL functions)
├── create_income_statement_view()
├── create_balance_sheet_view()
└── create_cash_flow_statement_view()


## 🚀 Getting Started

### Prerequisites

```bash
Python >= 3.8
PostgreSQL >= 13 (with uuid-ossp extension)
Installation
bash
# Clone the repository
git clone https://github.com/mithunthakkar8/SEC-XBRL-Data-Platform
cd SEC-XBRL-Data-Platform

# Install Python dependencies
pip install arelle psycopg2-binary httpx lxml pycountry yahooquery sentence-transformers
Database Setup
sql
-- Run setup scripts in order
\i '101. Create Database.sql'
\i '102. Create Role.sql'
\i '103. Access and Privileges.sql'
\i '104. Schema Setup.sql'
\i '105. XBRL Schema Table Definitions.sql'
Usage Example
python
from XBRLToPostgresLoader import XBRLToPostgresLoader

db_config = {
    'dbname': 'finhub',
    'user': 'finhub_admin',
    'password': 'your_password_here',
    'host': 'localhost',
    'port': '5432'
}

loader = XBRLToPostgresLoader(db_config=db_config)

# Load a single filing
instance_file = "path/to/filing.xml"
submission_file = "path/to/submission.txt"

if loader.load_xbrl_file(instance_file):
    company_id = loader.load_metadata(submission_file)
    fact_count = loader._process_standard_facts(company_id=company_id)
    print(f"Processed {fact_count} facts")
Running the Full Pipeline
python
from SECFilingPipeline import SECFilingPipeline

config = {
    'cik': '0001001838',
    'base_save_dir': 'filings',
    'db_config': {...},
    'years': {2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025},
    'count': 100
}

pipeline = SECFilingPipeline(**config)
pipeline.run_pipeline()  # Scrapes → discovers → processes all filings for the CIK
Generating Financial Statement Views
sql
-- Income Statement (pivoted)
SELECT company.create_income_statement_view('FCX', 'NYQ', debug := true);

-- Balance Sheet (pivoted)
SELECT company.create_balance_sheet_view('FCX', 'NYQ', debug := true);

-- Cash Flow Statement (normalized unpivoted)
SELECT company.create_cash_flow_statement_view_UP('FCX', 'NYQ', debug := true);
📁 Project Structure

├── SECScraper.py                 # SEC EDGAR scraping with rate limiting
├── XBRLToPostgresLoader.py       # Core ETL loader (Arelle + PostgreSQL)
├── SECFilingPipeline.py          # Orchestrates scraping + loading
├── Helper_Functions.py           # LEI, ISO codes, ticker lookup, Yahoo enrichment
├── Validation_Class.py           # Legacy XBRL processor (deprecated features)
├── sql/
│   ├── 101-105. Core setup       # Database, role, schema, tables
│   ├── 106-112. Business views   # Financial statement functions
│   └── company/                  # Concept mapping tables
🔐 Environment Security
Critical: The code contains hardcoded database credentials (pass@123). Before publishing:

Remove or replace with environment variables

Never commit real credentials

Example fix:

python
import os
db_config = {
    'password': os.environ.get('DB_PASSWORD'),
    # ...
}
📄 License
This project is licensed under the GNU General Public License v3.0.
See the LICENSE file for details.

📝 Citation
If you use this software in your research or production environment, please cite:

Mithun Thakkar, "SEC XBRL Data Warehousing Pipeline", Zenodo, 2026

bibtex
@software{Thakkar_SEC_XBRL_Data_Warehousing_2026,
  author = {Mithun Thakkar},
  title = {SEC XBRL Data Warehousing Pipeline},
  url = {https://github.com/YOUR_USERNAME/YOUR_REPO_NAME},
  doi = {10.5281/zenodo.YOUR-DOI},
  version = {1.0.0},
  year = {2026}
}




⚠️ Important Notes
Issue	Recommendation
Hardcoded credentials	Replace with environment variables or secrets manager
SEC rate limits	Scraper enforces 8-9 requests/second; do not modify
Arelle version	Tested with Arelle 2.0+
PostgreSQL extensions	Requires uuid-ossp and pldbgapi
📧 Contact
For questions, collaborations, or access inquiries:

mithun.thakkar8@gmail.com

Disclaimer: This software is for academic and research purposes. Users are responsible for complying with SEC's terms of service and rate limiting policies.
