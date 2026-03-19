-- Stock Prices Table
CREATE TABLE IF NOT EXISTS alphascope.stock_prices (
    stock_price_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    company_id UUID REFERENCES alphascope.company(company_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    volume NUMERIC,
    adjusted_close NUMERIC,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (company_id, date)
);