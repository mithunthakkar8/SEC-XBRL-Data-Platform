-- Balance Sheets with restatements (parameterized version) with debugging
CREATE OR REPLACE FUNCTION company.create_balance_sheet_view(
    p_ticker_symbol text,
    p_exchange_code char(4),
    p_debug boolean DEFAULT false
) 
RETURNS void AS $$
DECLARE
    dynamic_sql text;
    column_list text;
    view_name text;
    company_identifier text;
    debug_info text;
    concept_count integer;
    filing_count integer;
	success boolean;
BEGIN
    -- Create view name based on ticker symbol and exchange code
    company_identifier := format('%s_%s', lower(p_ticker_symbol), lower(p_exchange_code));
    view_name := format('company.%s_balance_sheet', company_identifier);
    
    -- Debug output if enabled
    IF p_debug THEN
        RAISE NOTICE 'Starting balance sheet view creation for % (ticker: %, exchange: %)', 
            company_identifier, p_ticker_symbol, p_exchange_code;
    END IF;
    
    -- Drop existing view if it exists
    EXECUTE format('DROP VIEW IF EXISTS %s', view_name);
    IF p_debug THEN
        RAISE NOTICE 'Dropped existing view % if it existed', view_name;
    END IF;

    -- Build the column list for the pivot with a single query
    SELECT string_agg(
        format(
            'MAX(CASE WHEN normalized_concept_name = %L THEN numeric_value END) AS %s',
            normalized_concept_name,
            -- Original regex: Convert camelCase to snake_case and truncate to 63 chars
            lower(substring(regexp_replace(normalized_concept_name, '([a-z])([A-Z])', '\1_\2', 'g'), 1, 63))
        ),
        ', '
    ) INTO column_list
    FROM (
        SELECT DISTINCT cnm.normalized_concept_name
        FROM xbrl.reported_fact rf 
        JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
        JOIN xbrl.concept_relationship cr ON cr.child_name = cpt.concept_name 
                                       AND cr.child_ns = cpt.namespace
        JOIN xbrl.filing f ON rf.filing_id = f.filing_id
        JOIN xbrl.company cny ON f.company_id = cny.company_id
        JOIN company.BS_concept_name_mapping cnm ON cnm.original_concept_name = cpt.concept_name
        JOIN xbrl.context c ON rf.context_id = c.context_id
        JOIN xbrl.context_period cp ON c.period_id = cp.context_period_id
        WHERE cr.role_id IN (
            SELECT role_id
            FROM xbrl.link_role
            WHERE role_uri ~* 'ConsolidatedBalanceSheets?'
        )
        AND rf.has_segment = FALSE
        AND cny.ticker_symbol = p_ticker_symbol
        AND cny.exchange_code = p_exchange_code
    ) AS distinct_concepts;
    
    -- Debug: Show number of distinct concepts found (with complete JOINs)
    IF p_debug THEN
        EXECUTE format('
            SELECT COUNT(*) 
            FROM (
                SELECT DISTINCT cnm.normalized_concept_name
                FROM xbrl.reported_fact rf 
                JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
                JOIN xbrl.concept_relationship cr ON cr.child_name = cpt.concept_name 
                                               AND cr.child_ns = cpt.namespace
                JOIN xbrl.filing f ON rf.filing_id = f.filing_id
                JOIN xbrl.company cny ON f.company_id = cny.company_id
                JOIN company.BS_concept_name_mapping cnm ON cnm.original_concept_name = cpt.concept_name
                JOIN xbrl.context c ON rf.context_id = c.context_id
                JOIN xbrl.context_period cp ON c.period_id = cp.context_period_id
                WHERE cr.role_id IN (
                    SELECT role_id
                    FROM xbrl.link_role
                    WHERE role_uri ~* ''ConsolidatedBalanceSheets?''
                )
                AND rf.has_segment = FALSE
                AND cny.ticker_symbol = %L
                AND cny.exchange_code = %L
            ) AS temp',
            p_ticker_symbol, p_exchange_code
        ) INTO concept_count;
        
        RAISE NOTICE 'Found % distinct balance sheet concepts for company %', concept_count, company_identifier;
    END IF;
    
    -- Failsafe: Check if column_list is empty or null
    IF column_list IS NULL OR column_list = '' THEN
        -- Enhanced error message with debug info
        debug_info := format('
            No balance sheet columns found to pivot for company % (ticker: %, exchange: %).
            Possible issues:
            1. Company not found with these identifiers
            2. No balance sheet data available
            3. No matching concepts in BS_concept_name_mapping
            For debugging, try:
            SELECT * FROM xbrl.company WHERE ticker_symbol = %L AND exchange_code = %L;
            SELECT COUNT(*) FROM xbrl.filing f JOIN xbrl.company c ON f.company_id = c.company_id 
                WHERE c.ticker_symbol = %L AND c.exchange_code = %L;
        ', company_identifier, p_ticker_symbol, p_exchange_code, 
           p_ticker_symbol, p_exchange_code, p_ticker_symbol, p_exchange_code);
        
        RAISE EXCEPTION '%', debug_info;
    END IF;
    
    -- Debug: Output column list if enabled
    IF p_debug THEN
        RAISE NOTICE 'Generated column list with % concepts', array_length(string_to_array(column_list, ','), 1);
        -- For very large column lists, you might want to limit this output
        IF array_length(string_to_array(column_list, ','), 1) < 20 THEN
            RAISE NOTICE 'Column list: %', column_list;
        END IF;
    END IF;
    
    -- Build the dynamic SQL with only period columns and pivoted concepts
    dynamic_sql := format('
        CREATE VIEW %s AS
        WITH latest_filings AS (
            SELECT 
                cpt.concept_name,
                cp.instant_date,
                MAX(f.filing_date) AS latest_filing_date
            FROM xbrl.reported_fact rf 
            JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
            JOIN xbrl.concept_relationship cr ON cr.child_name = cpt.concept_name 
                                           AND cr.child_ns = cpt.namespace
            JOIN xbrl.filing f ON rf.filing_id = f.filing_id
            JOIN xbrl.company cny ON f.company_id = cny.company_id
            JOIN xbrl.context c ON rf.context_id = c.context_id
            JOIN xbrl.context_period cp ON c.period_id = cp.context_period_id
            WHERE cr.role_id IN (
                SELECT role_id
                FROM xbrl.link_role
                WHERE role_uri ~* ''ConsolidatedBalanceSheets?''
            )
            AND rf.has_segment = FALSE
            AND cny.ticker_symbol = %L
            AND cny.exchange_code = %L
            GROUP BY cpt.concept_name, cp.instant_date
        )
        SELECT 
            cp.instant_date,
            cny.name AS company_name,
            %s
        FROM xbrl.reported_fact rf 
        JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
        JOIN company.BS_concept_name_mapping cnm ON cnm.original_concept_name = cpt.concept_name
        JOIN xbrl.filing f ON rf.filing_id = f.filing_id
        JOIN xbrl.company cny ON f.company_id = cny.company_id
        JOIN xbrl.context c ON rf.context_id = c.context_id
        JOIN xbrl.context_period cp ON c.period_id = cp.context_period_id
        JOIN latest_filings lf ON lf.concept_name = cpt.concept_name
                              AND lf.instant_date = cp.instant_date
                              AND lf.latest_filing_date = f.filing_date
        WHERE cny.ticker_symbol = %L
        AND cny.exchange_code = %L
        GROUP BY cp.instant_date, cny.name
    ', view_name, p_ticker_symbol, p_exchange_code, column_list, p_ticker_symbol, p_exchange_code);
    
    -- Debug: Show the generated SQL if enabled (truncated if very long)
    IF p_debug THEN
        RAISE NOTICE 'Generated dynamic SQL (first 500 chars): %', substring(dynamic_sql, 1, 500);
        IF length(dynamic_sql) > 500 THEN
            RAISE NOTICE '... (truncated, total length: %)', length(dynamic_sql);
        END IF;
    END IF;
    
    -- Execute the dynamic SQL
    EXECUTE dynamic_sql;
    
    -- Check if view was created successfully by querying it
    BEGIN
        EXECUTE format('SELECT 1 FROM %s LIMIT 1', view_name);
        success := true;
    EXCEPTION WHEN OTHERS THEN
        success := false;
    END;
    
    -- Count rows in the view
    IF success THEN
        EXECUTE format('SELECT COUNT(*) FROM %s', view_name) INTO filing_count;
        RAISE NOTICE 'Successfully created view % with % rows', view_name, filing_count;
    ELSE
        RAISE EXCEPTION 'Failed to create view %', view_name;
    END IF;
END;
$$ LANGUAGE plpgsql;
-- Corrected example usages with explicit type casting:

