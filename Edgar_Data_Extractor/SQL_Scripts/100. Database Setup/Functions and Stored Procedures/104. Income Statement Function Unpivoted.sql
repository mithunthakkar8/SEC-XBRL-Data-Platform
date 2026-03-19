-- Drop existing function if it exists to allow replacement
DROP FUNCTION IF EXISTS company.create_income_statement_view_UP;

CREATE OR REPLACE FUNCTION company.create_income_statement_view_UP(
    p_ticker_symbol text,
    p_exchange_code char(4),
    p_debug boolean DEFAULT false
) RETURNS text AS $$
DECLARE
    dynamic_sql text;
    view_name text;
    concept_count integer;
    data_count integer;
    view_check integer;
    company_id_val UUID;
    role_uri_pattern text := 'ConsolidatedStatements?Of(Income|Operations|Earnings)';
BEGIN
    -- Input validation
    IF p_ticker_symbol IS NULL OR trim(p_ticker_symbol) = '' THEN
        RETURN 'Error: Ticker symbol cannot be empty';
    END IF;
    IF p_exchange_code IS NULL OR trim(p_exchange_code) = '' THEN
        RETURN 'Error: Exchange code cannot be empty';
    END IF;

    view_name := format('company.%s_%s_income_statement_U', lower(p_ticker_symbol), lower(p_exchange_code));

    IF p_debug THEN
        RAISE NOTICE 'Starting view creation for % on %', p_ticker_symbol, p_exchange_code;
        RAISE NOTICE 'View name: %', view_name;
    END IF;

    -- Get company ID
    BEGIN
        SELECT company_id INTO company_id_val 
        FROM xbrl.company 
        WHERE ticker_symbol = p_ticker_symbol AND exchange_code = p_exchange_code;

        IF NOT FOUND THEN
            RETURN format('Error: Company with ticker %s on exchange %s not found', p_ticker_symbol, p_exchange_code);
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RETURN format('Error checking company: %s', SQLERRM);
    END;

    -- Check for concept mappings
    BEGIN
        SELECT COUNT(*) INTO concept_count
        FROM company.IS_concept_name_mapping
        WHERE ticker_symbol = p_ticker_symbol AND exchange_code = p_exchange_code;

        IF concept_count = 0 THEN
            RETURN format('Error: No income statement concepts mapped for ticker %s on exchange %s', p_ticker_symbol, p_exchange_code);
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RETURN format('Error checking concept mappings: %s', SQLERRM);
    END;

    -- Check for reported data
    BEGIN
        SELECT COUNT(*) INTO data_count
        FROM xbrl.reported_fact rf
        JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
        JOIN company.IS_concept_name_mapping cnm ON cnm.original_concept_name = cpt.concept_name
        JOIN xbrl.filing f ON rf.filing_id = f.filing_id
        WHERE f.company_id = company_id_val AND rf.has_segment = false;

        IF data_count = 0 THEN
            RETURN format('Error: No financial data found for ticker %s on exchange %s', p_ticker_symbol, p_exchange_code);
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RETURN format('Error checking data existence: %s', SQLERRM);
    END;

    -- Drop existing view
    BEGIN
        EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %s CASCADE', view_name);
    EXCEPTION WHEN OTHERS THEN
        RETURN format('Error dropping existing view: %s', SQLERRM);
    END;

    -- Build and execute dynamic SQL
    BEGIN
        dynamic_sql := format('
            CREATE MATERIALIZED VIEW %s AS
            WITH latest_filings AS (
                SELECT 
                    cpt.concept_name,
                    cp.period_start,
                    cp.period_end,
                    MAX(f.filing_date) AS max_filing_date
                FROM xbrl.reported_fact rf 
                JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
                JOIN xbrl.concept_relationship cr ON cr.child_name = cpt.concept_name AND cr.child_ns = cpt.namespace
                JOIN xbrl.link_role lr ON cr.role_id = lr.role_id
                JOIN xbrl.filing f ON rf.filing_id = f.filing_id
                JOIN xbrl.company cny ON cny.company_id = f.company_id
                JOIN xbrl.context c ON rf.context_id = c.context_id
                JOIN xbrl.context_period cp ON c.period_id = cp.context_period_id
                WHERE lr.role_uri ~* %L
                AND rf.has_segment = FALSE
                AND (cp.period_end - cp.period_start BETWEEN 360 AND 370 
                     OR cp.period_end - cp.period_start BETWEEN 85 AND 100)
                AND cny.ticker_symbol = %L
                AND cny.exchange_code = %L
                GROUP BY cpt.concept_name, cp.period_start, cp.period_end
            ),
            raw_data AS (
                SELECT DISTINCT
                    cnm.normalized_concept_name,
                    cp.period_start,
                    cp.period_end,
                    rf.numeric_value,
                    CASE 
                        WHEN cp.period_end - cp.period_start BETWEEN 360 AND 370 THEN ''Annual''
                        WHEN EXTRACT(MONTH FROM cp.period_end) = 4 THEN ''Q1''
                        WHEN EXTRACT(MONTH FROM cp.period_end) = 7 THEN ''Q2''
                        WHEN EXTRACT(MONTH FROM cp.period_end) = 10 THEN ''Q3''
                        WHEN EXTRACT(MONTH FROM cp.period_end) = 1 THEN ''Q4''
                        ELSE ''Other''
                    END AS period,
                    EXTRACT(YEAR FROM cp.period_start) AS report_year
                FROM xbrl.reported_fact rf 
                JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
                JOIN company.IS_concept_name_mapping cnm ON cnm.original_concept_name = cpt.concept_name
                JOIN xbrl.concept_attribute ca ON cpt.concept_id = ca.concept_id
                JOIN xbrl.filing f ON rf.filing_id = f.filing_id
                JOIN xbrl.context c ON rf.context_id = c.context_id
                JOIN xbrl.context_period cp ON c.period_id = cp.context_period_id
                JOIN latest_filings lf ON cpt.concept_name = lf.concept_name
                                       AND cp.period_start = lf.period_start
                                       AND cp.period_end = lf.period_end
                                       AND f.filing_date = lf.max_filing_date
                JOIN xbrl.company cny ON f.company_id = cny.company_id
                WHERE rf.has_segment = FALSE
                AND cny.ticker_symbol = %L
                AND cny.exchange_code = %L
            )
            SELECT 
                normalized_concept_name,
                period_start,
                period_end,
                period,
                report_year,
                numeric_value
            FROM raw_data
			where period != ''Annual''
            ORDER BY report_year, period, normalized_concept_name
        ', view_name, role_uri_pattern, p_ticker_symbol, p_exchange_code, p_ticker_symbol, p_exchange_code);

        IF p_debug THEN
            RAISE NOTICE 'Executing dynamic SQL: %', dynamic_sql;
        END IF;

        EXECUTE dynamic_sql;

        PERFORM 1 FROM information_schema.views 
        WHERE table_schema = 'company' 
        AND table_name = lower(p_ticker_symbol) || '_' || lower(p_exchange_code) || '_income_statement';

        IF NOT FOUND THEN
            RETURN 'Error: View creation failed - check permissions or SQL syntax';
        END IF;

        EXECUTE format('SELECT COUNT(*) FROM %s', view_name) INTO view_check;

        RETURN format('Successfully created view %s with %s rows', view_name, view_check);
    EXCEPTION WHEN OTHERS THEN
        RETURN format('Error creating view: %s', SQLERRM);
    END;
END;
$$ LANGUAGE plpgsql;
