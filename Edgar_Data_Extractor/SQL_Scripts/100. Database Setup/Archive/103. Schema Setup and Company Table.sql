CREATE SCHEMA IF NOT EXISTS alphascope;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Company Table
CREATE TABLE IF NOT EXISTS alphascope.company (
    company_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
	cik VARCHAR(10) UNIQUE NOT NULL,
    name TEXT NOT NULL UNIQUE,
    ticker_symbol TEXT NOT NULL UNIQUE,
	sic VARCHAR(10),
    industry TEXT,
    sector TEXT,
    created_at TIMESTAMP DEFAULT NOW()
)


-- select * from alphascope.company
-- delete from alphascope.company
