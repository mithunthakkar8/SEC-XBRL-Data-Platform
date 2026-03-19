
CREATE TABLE xbrl.industry_classification (
    classification_id UUID DEFAULT uuid_generate_v4() 
        CONSTRAINT pk_industry_classification PRIMARY KEY,  
    source TEXT NOT NULL 
        CONSTRAINT ck_valid_source 
        CHECK (source IN ('YAHOO', 'GICS', 'ICB', 'SIC', 'NAICS')),
    level TEXT NOT NULL 
        CONSTRAINT ck_valid_level 
        CHECK (level IN ('SECTOR', 'INDUSTRY_GROUP', 'INDUSTRY', 'SUB_INDUSTRY')),
    name TEXT NOT NULL,
    code TEXT,
    parent_id UUID 
        CONSTRAINT fk_parent_classification 
        REFERENCES xbrl.industry_classification(classification_id),
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_source_level_name 
        UNIQUE (source, level, name)
);

-- Company Table (Core Company Data)
CREATE TABLE IF NOT EXISTS xbrl.company (
    company_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    lei CHAR(20) CONSTRAINT valid_lei_format CHECK (lei ~ '^[A-Z0-9]{20}$'),  -- Named LEI format constraint
    name TEXT NOT NULL,
    country CHAR(2) NOT NULL CONSTRAINT valid_country_code CHECK (country ~ '^[A-Z]{2}$'),  -- Fixed to 2 letters
    ticker_symbol TEXT NOT NULL,
    exchange_code CHAR(4),
    primary_industry_classification_id UUID REFERENCES xbrl.industry_classification(classification_id),
    former_name TEXT,
    name_change_date DATE,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Named composite constraints
    CONSTRAINT unique_company_name_per_country UNIQUE (name, country),
    CONSTRAINT unique_ticker_per_exchange UNIQUE (ticker_symbol, exchange_code)
);

-- Junction Table
CREATE TABLE xbrl.company_industry_classification (
    company_id UUID 
        CONSTRAINT fk_cic_company 
        REFERENCES xbrl.company(company_id),
    classification_id UUID 
        CONSTRAINT fk_cic_classification 
        REFERENCES xbrl.industry_classification(classification_id),
	recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_company_industry_classification 
        PRIMARY KEY (company_id, classification_id)
);

-- Filing metadata
CREATE TABLE IF NOT EXISTS xbrl.filing (
    filing_id UUID DEFAULT uuid_generate_v4() 
        CONSTRAINT pk_filing PRIMARY KEY,
    company_id UUID 
        CONSTRAINT fk_filing_company 
        REFERENCES xbrl.company ON DELETE CASCADE,
    accession_number VARCHAR(20) 
        CONSTRAINT uq_accession_number UNIQUE,
    filing_date TIMESTAMP WITH TIME ZONE NOT NULL,
    filing_type VARCHAR(6),
    period_end TIMESTAMP WITH TIME ZONE NOT NULL,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_company_filing 
        UNIQUE (company_id, filing_date, filing_type)
);

CREATE TABLE IF NOT EXISTS xbrl.context_period (
    context_period_id UUID DEFAULT uuid_generate_v4() 
        CONSTRAINT pk_context_period PRIMARY KEY,
    filing_id UUID 
        CONSTRAINT fk_context_period_filing 
        REFERENCES xbrl.filing ON DELETE CASCADE,
	period_type varchar(10),
    period_start DATE,
    period_end DATE,
    instant_date DATE,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_valid_context CHECK (
        (period_start IS NOT NULL AND period_end IS NOT NULL AND instant_date IS NULL) OR
        (instant_date IS NOT NULL AND period_start IS NULL AND period_end IS NULL)
    )
);


-- Create partial unique indexes separately
CREATE UNIQUE INDEX unique_period_context ON xbrl.context_period(filing_id, period_start, period_end)
WHERE (period_start IS NOT NULL AND period_end IS NOT NULL);

CREATE UNIQUE INDEX unique_instant_context ON xbrl.context_period(filing_id, instant_date)
WHERE (instant_date IS NOT NULL);

-- Context table 
CREATE TABLE xbrl.context (
    context_id UUID DEFAULT uuid_generate_v4() 
        CONSTRAINT pk_context PRIMARY KEY,
    filing_id UUID NOT NULL
        CONSTRAINT fk_context_filing 
        REFERENCES xbrl.filing ON DELETE CASCADE,
    period_id UUID 
		CONSTRAINT fk_context_context_period 
        REFERENCES xbrl.context_period(context_period_id) ON DELETE CASCADE,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xbrl.dimension_declaration (
    dimension_id UUID DEFAULT uuid_generate_v4() 
        CONSTRAINT pk_dimension PRIMARY KEY,
    dimension_name TEXT NOT NULL, 
	source TEXT CHECK (source IN ('Segment', 'Scenario')),
	filing_id UUID NOT NULL
	CONSTRAINT fk_dim_declaration_filing_id 
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	-- CONSTRAINT uq_dimension_declaration 
	-- 		UNIQUE (dimension_name, filing_id)
);

CREATE TABLE IF NOT EXISTS xbrl.dimension_member (
    member_id UUID DEFAULT uuid_generate_v4() 
        CONSTRAINT pk_member PRIMARY KEY,
    dimension_id UUID 
        CONSTRAINT fk_member_dimension 
        REFERENCES xbrl.dimension_declaration ON DELETE CASCADE,
    member_qname TEXT NOT NULL,
	member_name TEXT NOT NULL,
	member_ns TEXT NOT NULL,
    description TEXT,
    is_exhaustive BOOLEAN,
	is_default BOOLEAN,
	filing_id UUID NOT NULL
	CONSTRAINT fk_dim_member_filing_id 
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
	recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    -- CONSTRAINT uq_dimension_member 
    --     UNIQUE (dimension_id, member_qname, filing_id)
);


CREATE TABLE xbrl.context_dimension_members (
    cdm_id UUID DEFAULT uuid_generate_v4() 
        CONSTRAINT pk_cdm PRIMARY KEY,
    context_id UUID
		CONSTRAINT fk_cdm_context
        REFERENCES xbrl.context ON DELETE CASCADE,
    dimension_id UUID 
        CONSTRAINT fk_cdm_dimension 
        REFERENCES xbrl.dimension_declaration ON DELETE CASCADE,
    member_id UUID 
		CONSTRAINT fk_cdm_member
        REFERENCES xbrl.dimension_member ON DELETE CASCADE,
	filing_id UUID NOT NULL
	CONSTRAINT fk_cdm_filing_id 
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE
	-- CONSTRAINT uq_cdm_context_dimension 
 --    	UNIQUE(context_id, dimension_id, filing_id)
);

CREATE TABLE IF NOT EXISTS xbrl.concept (
    concept_id UUID DEFAULT uuid_generate_v4() 
        CONSTRAINT pk_concept PRIMARY KEY,
    concept_name TEXT NOT NULL,
    namespace TEXT NOT NULL,
	concept_qname TEXT NOT NULL,
	filing_id UUID NOT NULL
	CONSTRAINT fk_concept_filing_id 
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
	recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_concept_namespace 
        UNIQUE (concept_name, namespace, filing_id)
);

CREATE TABLE IF NOT EXISTS xbrl.label (
    concept_id UUID 
        CONSTRAINT pk_label PRIMARY KEY 
        REFERENCES xbrl.concept,
    standard_label TEXT NOT NULL,
    verbose_label TEXT,
    documentation TEXT,
	filing_id UUID NOT NULL
	CONSTRAINT fk_label_filing_id 
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS xbrl.concept_attribute (
    concept_id UUID 
        CONSTRAINT pk_concept_attribute PRIMARY KEY 
        REFERENCES xbrl.concept,
    period_type TEXT 
        CONSTRAINT ck_period_type 
        CHECK (period_type IN ('duration', 'instant')),
    data_type TEXT NOT NULL,
    balance_type TEXT 
        CONSTRAINT ck_balance_type 
        CHECK (balance_type IN ('debit', 'credit')),
    filing_id UUID NOT NULL
	CONSTRAINT fk_concept_attribute_filing_id 
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);



-- Note: The reported_fact table allows duplicate values in all columns. We hence have filing_id in it. 
-- In python - while inserting into this table for the same filing 
-- we first delete all records pertaining to the said filing_id
-- and then insert new records. 
CREATE TABLE IF NOT EXISTS xbrl.reported_fact (
    fact_id UUID DEFAULT uuid_generate_v4() 
        CONSTRAINT pk_fact PRIMARY KEY,
    context_id UUID 
        CONSTRAINT fk_fact_context 
        REFERENCES xbrl.context ON DELETE CASCADE,
    concept_id UUID 
        CONSTRAINT fk_fact_concept 
        REFERENCES xbrl.concept ON DELETE CASCADE,
    numeric_value NUMERIC,
    string_value TEXT,
    boolean_value BOOLEAN,
    date_value DATE,
	presentation_path TEXT,
	filing_id UUID NOT NULL
	CONSTRAINT fk_fact_filing_id 
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
	has_segment BOOLEAN,
	has_scenario BOOLEAN,
	decimals INTEGER,
    unit TEXT,
	company_id UUID
	CONSTRAINT fk_fact_company 
        REFERENCES xbrl.company(company_id),
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


-- Link roles (presentation networks)
CREATE TABLE xbrl.link_role (
    role_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    role_uri TEXT NOT NULL 
		CONSTRAINT uq_role_uri
        UNIQUE,
    role_definition TEXT,
	filing_id UUID NOT NULL
	CONSTRAINT fk_link_role_filing_id 
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Arcroles (relationship types)
CREATE TABLE xbrl.arcrole (
    arcrole_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    arcrole_uri TEXT NOT NULL 
		CONSTRAINT uq_arcrole_uri
        UNIQUE,
	filing_id UUID NOT NULL
	CONSTRAINT fk_arcrole_filing_id 
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
    arcrole_definition TEXT,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Concept relationships (core table)
CREATE TABLE xbrl.concept_relationship (
    relationship_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    parent_name TEXT NOT NULL,
	parent_ns TEXT NOT NULL,
	parent_qname TEXT NOT NULL,
    child_name TEXT NOT NULL,
	child_ns TEXT NOT NULL,
	child_qname TEXT NOT NULL,
    arcrole_id UUID NOT NULL 
		CONSTRAINT fk_concept_arcrole 
        REFERENCES xbrl.arcrole(arcrole_id) ON DELETE CASCADE,
    role_id UUID 
		CONSTRAINT fk_concept_role 
        REFERENCES xbrl.link_role(role_id) ON DELETE SET NULL,
    order_ DECIMAL(10,2),
    weight DECIMAL(10,2),
    preferred_label TEXT,
    filing_id UUID NOT NULL
		CONSTRAINT fk_concept_filing_id 
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
	presentation_path TEXT,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_concept_relationship 
        UNIQUE (parent_name, parent_ns, child_name, child_ns, arcrole_id, role_id, filing_id)
);


-- Hypercube declarations
CREATE TABLE xbrl.hypercube_dimension (
	hypercube_dimension_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    parent_name TEXT NOT NULL,
	parent_ns TEXT NOT NULL,
	parent_qname TEXT NOT NULL,
    child_name TEXT NOT NULL,
	child_ns TEXT NOT NULL,
	child_qname TEXT NOT NULL,
	arcrole_id UUID NOT NULL 
	CONSTRAINT fk_hypercube_arcrole 
        REFERENCES xbrl.arcrole(arcrole_id) ON DELETE CASCADE,
	role_id UUID 
		CONSTRAINT fk_hypercube_dimension_role 
        REFERENCES xbrl.link_role(role_id) ON DELETE SET NULL,
	filing_id UUID 
	CONSTRAINT fk_hypercube_dimension_filing 
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
	presentation_path TEXT,
    is_abstract BOOLEAN NOT NULL DEFAULT FALSE,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT uq_hypercube_dimension 
        UNIQUE (parent_name, parent_ns, child_name, child_ns)
);

-- Dimension relationships
CREATE TABLE xbrl.dimension_domain (
    dimension_domain_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
	parent_name TEXT NOT NULL,
	parent_ns TEXT NOT NULL,
	parent_qname TEXT NOT NULL,
    child_name TEXT NOT NULL,
	child_ns TEXT NOT NULL,
	child_qname TEXT NOT NULL,
    arcrole_id UUID NOT NULL 
	CONSTRAINT fk_dim_domain_arcrole 
        REFERENCES xbrl.arcrole(arcrole_id) ON DELETE CASCADE,
    role_id UUID 
		CONSTRAINT fk_dimension_domain_role 
        REFERENCES xbrl.link_role(role_id) ON DELETE SET NULL,
	filing_id UUID NOT NULL
	CONSTRAINT fk_dim_domain_filing 
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
	presentation_path TEXT,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dimension_domain
        UNIQUE (parent_name, parent_ns, child_name, child_ns)
);

-- Dimension-member relationships
CREATE TABLE xbrl.domain_member (
    domain_member_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
	parent_name TEXT NOT NULL,
	parent_ns TEXT NOT NULL,
	parent_qname TEXT NOT NULL,
    child_name TEXT NOT NULL,
	child_ns TEXT NOT NULL,
	child_qname TEXT NOT NULL,
	arcrole_id UUID NOT NULL 
	CONSTRAINT fk_domain_member_arcrole 
        REFERENCES xbrl.arcrole(arcrole_id) ON DELETE CASCADE,
    role_id UUID 
		CONSTRAINT fk_domain_member_role 
        REFERENCES xbrl.link_role(role_id) ON DELETE SET NULL,
	filing_id UUID NOT NULL
	CONSTRAINT fk_domain_member_filing
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
	presentation_path TEXT,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_domain_member
        UNIQUE (parent_name, parent_ns, child_name, child_ns)
);

-- All-data relationships
CREATE TABLE xbrl.all_data (
    parent_name TEXT NOT NULL,
	parent_ns TEXT NOT NULL,
	parent_qname TEXT NOT NULL,
    child_name TEXT NOT NULL,
	child_ns TEXT NOT NULL,
	child_qname TEXT NOT NULL,
	arcrole_id UUID NOT NULL 
	CONSTRAINT fk_all_data_arcrole 
        REFERENCES xbrl.arcrole(arcrole_id) ON DELETE CASCADE,
    role_id UUID 
		CONSTRAINT fk_all_data_role 
        REFERENCES xbrl.link_role(role_id) ON DELETE SET NULL,
	filing_id UUID NOT NULL
	CONSTRAINT fk_all_data_filing 
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
	presentation_path TEXT,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_all_data
        PRIMARY KEY (parent_name, parent_ns, child_name, child_ns)
);


-- notAll-data relationships
CREATE TABLE xbrl.notAll_data (
    parent_name TEXT NOT NULL,
	parent_ns TEXT NOT NULL,
    parent_qname TEXT NOT NULL,
    child_name TEXT NOT NULL,
	child_ns TEXT NOT NULL,
	child_qname TEXT NOT NULL,
	arcrole_id UUID NOT NULL 
	CONSTRAINT fk_notAll_data_arcrole 
        REFERENCES xbrl.arcrole(arcrole_id) ON DELETE CASCADE,
    role_id UUID 
		CONSTRAINT fk_notAll_data_role 
        REFERENCES xbrl.link_role(role_id) ON DELETE SET NULL,
	filing_id UUID NOT NULL
	CONSTRAINT fk_notAll_data_filing 
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
	presentation_path TEXT,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_notAll_data
        PRIMARY KEY (parent_name, parent_ns, child_name, child_ns)
);

-- dimension-default relationships
CREATE TABLE xbrl.Dimension_Default (
    parent_name TEXT NOT NULL,
	parent_ns TEXT NOT NULL,
    parent_qname TEXT NOT NULL,
    child_name TEXT NOT NULL,
	child_ns TEXT NOT NULL,
	child_qname TEXT NOT NULL,
	arcrole_id UUID NOT NULL 
	CONSTRAINT fk_dim_default_arcrole 
        REFERENCES xbrl.arcrole(arcrole_id) ON DELETE CASCADE,
    role_id UUID 
		CONSTRAINT fk_dimension_default_role 
        REFERENCES xbrl.link_role(role_id) ON DELETE SET NULL,
	filing_id UUID NOT NULL
	CONSTRAINT fk_dim_default_filing
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
	presentation_path TEXT,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_Dimension_Default
        PRIMARY KEY (parent_name, parent_ns, child_name, child_ns)
);

-- Explanatory Fact relationships
CREATE TABLE xbrl.Explanatory_Fact (
    parent_name TEXT NOT NULL,
	parent_ns TEXT NOT NULL,
    parent_qname TEXT NOT NULL,
    child_name TEXT NOT NULL,
	child_ns TEXT NOT NULL,
	child_qname TEXT NOT NULL,
	arcrole_id UUID NOT NULL 
	CONSTRAINT fk_explanatory_fact_arcrole 
        REFERENCES xbrl.arcrole(arcrole_id) ON DELETE CASCADE,
    role_id UUID 
		CONSTRAINT fk_explanatory_fact_role 
        REFERENCES xbrl.link_role(role_id) ON DELETE SET NULL,
	filing_id UUID NOT NULL
	CONSTRAINT fk_explanatory_fact_filing
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
	presentation_path TEXT,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_Explanatory_Fact
        PRIMARY KEY (parent_name, parent_ns, child_name, child_ns)
);




CREATE TABLE xbrl.calculation_relationship (
    parent_name TEXT NOT NULL,
	parent_ns TEXT NOT NULL,
    parent_qname TEXT NOT NULL,
    child_name TEXT NOT NULL,
	child_ns TEXT NOT NULL,
	child_qname TEXT NOT NULL,
	arcrole_id UUID NOT NULL 
	CONSTRAINT fk_calculation_arcrole 
        REFERENCES xbrl.arcrole(arcrole_id) ON DELETE CASCADE,
    role_id UUID 
		CONSTRAINT fk_calculation_relationship_role 
        REFERENCES xbrl.link_role(role_id) ON DELETE SET NULL,
	filing_id UUID NOT NULL
	CONSTRAINT fk_calculation_filing
        REFERENCES xbrl.filing(filing_id) ON DELETE CASCADE,
	presentation_path TEXT,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_calculation_relationship
        PRIMARY KEY (parent_name, parent_ns, child_name, child_ns)
);


-- CREATE TABLE xbrl.footnote (
--     footnote_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
--     fact_id UUID REFERENCES xbrl.reported_fact ON DELETE CASCADE,
--     content TEXT NOT NULL,
--     language_code CHAR(2) DEFAULT 'en',
--     recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
-- );

-- CREATE TABLE xbrl.footnote_link (
--     link_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
--     footnote_id UUID REFERENCES xbrl.footnote ON DELETE CASCADE,
--     role_id UUID REFERENCES xbrl.link_role,
--     title TEXT,
--     recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
-- );




-- -- Drop tables in reverse dependency order
-- DROP TABLE IF EXISTS xbrl.concept_relationship cascade;
-- DROP TABLE IF EXISTS xbrl.dimension_domain;
-- DROP TABLE IF EXISTS xbrl.hypercube_dimension;
-- DROP TABLE IF EXISTS xbrl.domain_member;
-- DROP TABLE IF EXISTS xbrl.all_data;
-- DROP TABLE IF EXISTS xbrl.notAll_data;
-- DROP TABLE IF EXISTS xbrl.Dimension_Default;
-- DROP TABLE IF EXISTS xbrl.Explanatory_Fact;
-- DROP TABLE IF EXISTS xbrl.calculation_relationship;
-- DROP TABLE IF EXISTS xbrl.arcrole;
-- DROP TABLE IF EXISTS xbrl.footnote_link;
-- DROP TABLE IF EXISTS xbrl.footnote;
-- DROP TABLE IF EXISTS xbrl.link_role;
-- DROP TABLE IF EXISTS xbrl.reported_fact;
-- DROP TABLE IF EXISTS xbrl.context_dimension_members;
-- DROP TABLE IF EXISTS xbrl.dimension_member;
-- DROP TABLE IF EXISTS xbrl.dimension_declaration;
-- DROP TABLE IF EXISTS xbrl.concept_attribute;
-- DROP TABLE IF EXISTS xbrl.label;
-- DROP TABLE IF EXISTS xbrl.context;
-- DROP TABLE IF EXISTS xbrl.context_period;
-- DROP TABLE IF EXISTS xbrl.concept;
-- DROP TABLE IF EXISTS xbrl.filing;
-- DROP TABLE IF EXISTS xbrl.company_industry_classification;
-- DROP TABLE IF EXISTS xbrl.company cascade;
-- DROP TABLE IF EXISTS xbrl.industry_classification;


