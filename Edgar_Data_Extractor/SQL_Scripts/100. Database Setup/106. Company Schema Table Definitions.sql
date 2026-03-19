-- Income Statement Concept Mappings
DROP TABLE IF EXISTS company.IS_concept_name_mapping cascade;
CREATE TABLE company.IS_concept_name_mapping (
    original_concept_name TEXT, 
    normalized_concept_name TEXT,
	ticker_symbol TEXT,
	exchange_code CHAR(4),
		CONSTRAINT fk_ticker_exchange_company_IS_concept FOREIGN KEY (ticker_symbol, exchange_code)
    	REFERENCES xbrl.company (ticker_symbol, exchange_code),
	company_id UUID 
    	CONSTRAINT fk_IS_concept_name_mapping_company
    	REFERENCES xbrl.company(company_id),
	CONSTRAINT pk_IS_concept_name_company 
        PRIMARY KEY (original_concept_name, company_id)
);

-- Balance Sheet Concept Mappings
DROP TABLE IF EXISTS company.BS_concept_name_mapping cascade;
CREATE TABLE company.BS_concept_name_mapping (
	original_concept_name TEXT,
    normalized_concept_name TEXT,
	ticker_symbol TEXT,
	exchange_code CHAR(4),
		CONSTRAINT fk_ticker_exchange_company_BS_concept FOREIGN KEY (ticker_symbol, exchange_code)
    	REFERENCES xbrl.company (ticker_symbol, exchange_code),
	company_id UUID 
    	CONSTRAINT fk_BS_concept_name_mapping_company
    	REFERENCES xbrl.company(company_id),
	CONSTRAINT pk_BS_concept_name_company 
        Primary KEY (original_concept_name, company_id)
);

-- Cash Flow Statement Mappings
DROP TABLE IF EXISTS company.CF_concept_name_mapping CASCADE;
CREATE TABLE company.CF_concept_name_mapping (
    original_concept_name TEXT, 
    normalized_concept_name TEXT,
	ticker_symbol TEXT,
	exchange_code CHAR(4),
		CONSTRAINT fk_ticker_exchange_company_CF_concept FOREIGN KEY (ticker_symbol, exchange_code)
    	REFERENCES xbrl.company (ticker_symbol, exchange_code),
	company_id UUID 
    	CONSTRAINT fk_CF_concept_name_mapping_company
    	REFERENCES xbrl.company(company_id),
	CONSTRAINT pk_CF_concept_name_company 
        PRIMARY KEY (original_concept_name, company_id)
);


-- Segment History
CREATE TABLE company.segment_history (
    segment_history_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID 
	CONSTRAINT fk_CF_concept_name_mapping_company
    	REFERENCES xbrl.company(company_id),
    concept_name TEXT NOT NULL,
    parent_qname TEXT NOT NULL,
    member_name TEXT,
    period_start DATE,
    period_end DATE,
    instant_date DATE,
    numeric_value NUMERIC(20,4),
    balance_type VARCHAR(20),
    filing_date TIMESTAMP WITH TIME ZONE NOT NULL,
	period_type VARCHAR(10),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_segment_history_company ON company.segment_history(company_id);
CREATE INDEX idx_segment_history_period ON company.segment_history(period_start, period_end);
CREATE INDEX idx_segment_history_concept ON company.segment_history(concept_name);
CREATE INDEX idx_segment_history_filing_date ON company.segment_history(filing_date);


