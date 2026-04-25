# SEC XBRL Data Warehousing Pipeline

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19773629.svg)](https://doi.org/10.5281/zenodo.19773629)

## 📌 Overview

This project implements an **end-to-end ETL pipeline** for extracting, transforming, and loading SEC XBRL financial filings into a PostgreSQL data warehouse and generates analytics in Power BI. It includes:

- Automated scraping of SEC EDGAR  
- XBRL parsing using Arelle  
- Data enrichment (LEI, Yahoo Finance, SEC API)  
- Analytical views for financial statements
- Power BI Period over period changes in statement numbers

---

## 🚀 Key Features

| Feature | Description |
|--------|------------|
| **Automated SEC Scraping** | Rate-limited scraping of 10-K, 10-Q filings with retry logic |
| **XBRL Parsing** | Full taxonomy loading, fact extraction, context preservation |
| **Data Enrichment** | LEI (GLEIF), ticker (SEC API), metadata (Yahoo Finance) |
| **PostgreSQL Warehouse** | Normalized schema for filings, facts, relationships |
| **Financial Statement Views** | Dynamic pivot views (IS, BS, CF) |
| **Segment Support** | Handles dimensional contexts |

---

## 🛠️ Tech Stack

| Category | Technologies |
|----------|-------------|
| Languages | Python, SQL, PL/pgSQL |
| XBRL | Arelle |
| Scraping | httpx, lxml, requests |
| Database | PostgreSQL |
| APIs | GLEIF, SEC EDGAR, Yahoo Finance |
| Embeddings | Sentence Transformers |

---

## 📊 Data Warehouse Schema

- `xbrl.industry_classification`
- `xbrl.company`
- `xbrl.filing`
- `xbrl.context_period`
- `xbrl.context`
- `xbrl.dimension_declaration`
- `xbrl.dimension_member`
- `xbrl.concept`
- `xbrl.label`
- `xbrl.concept_attribute`
- `xbrl.reported_fact`
- `xbrl.concept_relationship`

---

## 🔄 Pipeline Architecture

```text
SEC EDGAR
   │
   ▼
SECScraper.py
   │
   ▼
Raw filings (.txt + .xml)
   │
   ▼
XBRLToPostgresLoader.py
   ├── load_metadata()
   ├── load_xbrl_file()
   ├── load_concepts_batch()
   ├── load_contexts_batch()
   ├── _process_standard_facts()
   └── process_xbrl_relationships()
   │
   ▼
PostgreSQL Data Warehouse
   │
   ▼
Financial Statement Views
   │
   ▼
Power BI


🚀 Getting Started
Prerequisites
Python >= 3.8
PostgreSQL >= 13 (with uuid-ossp extension)
Installation
git clone https://github.com/mithunthakkar8/SEC-XBRL-Data-Platform
cd SEC-XBRL-Data-Platform

pip install arelle psycopg2-binary httpx lxml pycountry yahooquery sentence-transformers
Database Setup
\i '101. Create Database.sql'
\i '102. Create Role.sql'
\i '103. Access and Privileges.sql'
\i '104. Schema Setup.sql'
\i '105. XBRL Schema Table Definitions.sql'
🧪 Usage Example
from XBRLToPostgresLoader import XBRLToPostgresLoader

db_config = {
    'dbname': 'finhub',
    'user': 'finhub_admin',
    'password': 'your_password_here',
    'host': 'localhost',
    'port': '5432'
}

loader = XBRLToPostgresLoader(db_config=db_config)

instance_file = "path/to/filing.xml"
submission_file = "path/to/submission.txt"

if loader.load_xbrl_file(instance_file):
    company_id = loader.load_metadata(submission_file)
    fact_count = loader._process_standard_facts(company_id=company_id)
    print(f"Processed {fact_count} facts")
🔄 Running Full Pipeline
from SECFilingPipeline import SECFilingPipeline

config = {
    'cik': '0001001838',
    'base_save_dir': 'filings',
    'db_config': db_config,
    'years': {2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025},
    'count': 100
}

pipeline = SECFilingPipeline(**config)
pipeline.run_pipeline()
📊 Financial Statement Views
SELECT company.create_income_statement_view('FCX', 'NYQ', debug := true);
SELECT company.create_balance_sheet_view('FCX', 'NYQ', debug := true);
SELECT company.create_cash_flow_statement_view_UP('FCX', 'NYQ', debug := true);
📁 Project Structure
├── SECScraper.py
├── XBRLToPostgresLoader.py
├── SECFilingPipeline.py
├── Helper_Functions.py
├── Validation_Class.py
├── sql/
│   ├── 101-105. Core setup
│   ├── 106-112. Business views
│   └── company/
🔐 Security

⚠️ Important

Remove hardcoded credentials
Use environment variables
Never commit secrets
import os

db_config = {
    'password': os.environ.get('DB_PASSWORD')
}
📄 License

GNU General Public License v3.0

📝 Citation
@software{Thakkar_SEC_XBRL_Data_Warehousing_2026,
  author = {Mithun Thakkar},
  title = {SEC XBRL Data Warehousing Pipeline},
  doi = {10.5281/zenodo.19773629},
  year = {2026}
}
⚠️ Important Notes
Issue	Recommendation
Credentials	Use env variables
SEC limits	Keep 8–9 req/sec
Arelle	Use v2.0+
PostgreSQL	Requires uuid-ossp
📧 Contact

mithun.thakkar8@gmail.com
