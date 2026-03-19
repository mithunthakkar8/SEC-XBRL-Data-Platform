DROP FUNCTION IF EXISTS company.create_balance_sheet_view_UP;

CREATE OR REPLACE FUNCTION company.create_balance_sheet_view_UP(
    p_ticker_symbol text,
    p_exchange_code char(4),
    p_debug boolean DEFAULT false
) RETURNS void AS $$
DECLARE
    view_name text;
    dynamic_sql text;
    company_identifier text;
    row_count integer;
BEGIN
    -- Generate view name
    company_identifier := format('%s_%s', lower(p_ticker_symbol), lower(p_exchange_code));
    view_name := format('company.%s_balance_sheet_UP', company_identifier);

    IF p_debug THEN
        RAISE NOTICE 'Creating balance sheet view: %', view_name;
    END IF;

    -- Drop existing view if it exists
    EXECUTE format('DROP VIEW IF EXISTS %s CASCADE', view_name);

    -- Build the dynamic SQL for normalized output
    dynamic_sql := format($sql$
        CREATE VIEW %s AS
        WITH latest_filings AS (
            SELECT 
                cpt.concept_name,
                cp.instant_date,
                MAX(f.filing_date) AS latest_filing_date
            FROM xbrl.reported_fact rf 
            JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
            JOIN xbrl.concept_relationship cr ON cr.child_name = cpt.concept_name AND cr.child_ns = cpt.namespace
            JOIN xbrl.link_role lr ON cr.role_id = lr.role_id
            JOIN xbrl.filing f ON rf.filing_id = f.filing_id
            JOIN xbrl.company cny ON f.company_id = cny.company_id
            JOIN xbrl.context c ON rf.context_id = c.context_id
            JOIN xbrl.context_period cp ON c.period_id = cp.context_period_id
            WHERE lr.role_uri ~* 'ConsolidatedBalanceSheets?'
              AND rf.has_segment = FALSE
              AND cny.ticker_symbol = %L
              AND cny.exchange_code = %L
            GROUP BY cpt.concept_name, cp.instant_date
        )
        SELECT 
            cp.instant_date,
            cnm.normalized_concept_name,
            rf.numeric_value,
            EXTRACT(YEAR FROM cp.instant_date) AS report_year,
            'Q' || CEIL(EXTRACT(MONTH FROM cp.instant_date) / 3.0)::text AS report_quarter
        FROM xbrl.reported_fact rf 
        JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
        JOIN xbrl.concept_attribute ca ON ca.concept_id = cpt.concept_id
        JOIN company.BS_concept_name_mapping cnm ON cnm.original_concept_name = cpt.concept_name
        JOIN xbrl.filing f ON rf.filing_id = f.filing_id
        JOIN xbrl.company cny ON f.company_id = cny.company_id
        JOIN xbrl.context c ON rf.context_id = c.context_id
        JOIN xbrl.context_period cp ON c.period_id = cp.context_period_id
        JOIN latest_filings lf ON lf.concept_name = cpt.concept_name
                               AND lf.instant_date = cp.instant_date
                               AND lf.latest_filing_date = f.filing_date
        WHERE rf.has_segment = FALSE
          AND cny.ticker_symbol = %L
          AND cny.exchange_code = %L
        ORDER BY cp.instant_date, cnm.normalized_concept_name
    $sql$, view_name, p_ticker_symbol, p_exchange_code, p_ticker_symbol, p_exchange_code);

    IF p_debug THEN
        RAISE NOTICE 'Executing dynamic SQL for normalized balance sheet view...';
    END IF;

    -- Execute view creation
    EXECUTE dynamic_sql;

    -- Check rows
    EXECUTE format('SELECT COUNT(*) FROM %s', view_name) INTO row_count;

    RAISE NOTICE 'Successfully created view % with % rows', view_name, row_count;
END;
$$ LANGUAGE plpgsql;
