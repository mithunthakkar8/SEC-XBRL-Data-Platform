CREATE OR REPLACE FUNCTION company.create_cash_flow_statement_view_UP(
    p_ticker_symbol VARCHAR,
    p_exchange_code VARCHAR,
    p_debug BOOLEAN DEFAULT FALSE
) RETURNS VOID AS $$
DECLARE
    dynamic_sql TEXT;
    view_name TEXT := format('%s_%s_cash_flow_statement_UP', 
                             lower(p_ticker_symbol), 
                             lower(p_exchange_code));
BEGIN
    IF p_debug THEN
        RAISE NOTICE 'Creating cash flow statement view: company.%', view_name;
    END IF;

    dynamic_sql := format($sql$
        DROP VIEW IF EXISTS company.%s;
        
        CREATE VIEW company.%s AS
        WITH latest_filing AS (
            SELECT 
                cpt.concept_name, 
                cp.period_start, 
                cp.period_end, 
                MAX(f.filing_date) AS max_filing_date
            FROM xbrl.filing f
            JOIN xbrl.context c ON f.filing_id = c.filing_id
            JOIN xbrl.context_period cp ON c.period_id = cp.context_period_id
            JOIN xbrl.reported_fact rf ON c.context_id = rf.context_id AND NOT rf.has_segment
            JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
            JOIN xbrl.company cny ON f.company_id = cny.company_id
            JOIN xbrl.concept_relationship cr ON cr.child_name = cpt.concept_name 
                                             AND cr.child_ns = cpt.namespace
            JOIN xbrl.link_role lr ON cr.role_id = lr.role_id
            WHERE lr.role_uri ~* 'consolidatedstatements?ofCASHFLOWS'
              AND cny.ticker_symbol = %L
              AND cny.exchange_code = %L
            GROUP BY cpt.concept_name, cp.period_start, cp.period_end
        ),
        
        cash_flow_data AS (
            SELECT 
                cnm.normalized_concept_name,
                cp.period_start,
                cp.period_end,
                rf.numeric_value,
                CASE 
                    WHEN cp.period_end - cp.period_start BETWEEN 85 AND 100 
						AND EXTRACT(MONTH from cp.period_start) BETWEEN 1 AND 3 THEN 'Q1'
					WHEN cp.period_end - cp.period_start BETWEEN 85 AND 100 
						AND EXTRACT(MONTH from cp.period_start) BETWEEN 4 AND 6 THEN 'Q2'
					WHEN cp.period_end - cp.period_start BETWEEN 85 AND 100 
						AND EXTRACT(MONTH from cp.period_start) BETWEEN 7 AND 9 THEN 'Q3'
					WHEN cp.period_end - cp.period_start BETWEEN 175 AND 185 THEN 'Q2YTD'
					WHEN cp.period_end - cp.period_start BETWEEN 265 AND 275 THEN 'Q3YTD'
					WHEN cp.period_end - cp.period_start BETWEEN 355 AND 370 THEN 'Annual'
					ELSE 'Other'
                END AS period_type,
                EXTRACT(YEAR FROM cp.period_start) AS report_year
            FROM xbrl.reported_fact rf 
            JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
            JOIN company.CF_concept_name_mapping cnm ON cnm.original_concept_name = cpt.concept_name
            JOIN xbrl.concept_attribute ca ON cpt.concept_id = ca.concept_id
            JOIN xbrl.context c ON rf.context_id = c.context_id
            JOIN xbrl.context_period cp ON c.period_id = cp.context_period_id
            JOIN xbrl.filing f ON c.filing_id = f.filing_id
            JOIN xbrl.company cny ON cny.company_id = f.company_id
            JOIN latest_filing lf ON 
                cpt.concept_name = lf.concept_name AND
                cp.period_start = lf.period_start AND
                cp.period_end = lf.period_end AND
                f.filing_date = lf.max_filing_date
            WHERE rf.has_segment = FALSE
              AND cny.ticker_symbol = %L
              AND cny.exchange_code = %L
        ),

        materialized_data AS (
            WITH all_concepts AS (
                SELECT DISTINCT normalized_concept_name FROM cash_flow_data
            ),
            all_periods AS (
                SELECT DISTINCT 
                    period_start,
                    period_end,
                    period_type,
                    report_year
                FROM cash_flow_data
                WHERE period_type IN ('Q1', 'Q2YTD', 'Q3YTD', 'Annual')
            ),
            cross_join AS (
                SELECT 
                    c.normalized_concept_name,
                    p.period_start,
                    p.period_end,
                    p.period_type,
                    p.report_year
                FROM all_concepts c
                CROSS JOIN all_periods p
            )
            SELECT 
                cj.normalized_concept_name,
                cj.period_start,
                cj.period_end,
                COALESCE(cd.numeric_value, 0) AS numeric_value,
                cj.period_type,
                cj.report_year
            FROM cross_join cj
            LEFT JOIN cash_flow_data cd ON 
                cj.normalized_concept_name = cd.normalized_concept_name AND
                cj.period_start = cd.period_start AND
                cj.period_end = cd.period_end AND
                cj.period_type = cd.period_type AND
                cj.report_year = cd.report_year
        ),

        reported_periods AS (
            SELECT * FROM materialized_data
            WHERE period_type IN ('Q1', 'Q2YTD', 'Q3YTD', 'Annual')
        ),

        derived_q2 AS (
            SELECT 
                q1.normalized_concept_name,
                DATE(q1.report_year::text || '-04-01') AS period_start,
                DATE(q1.report_year::text || '-07-01') AS period_end,
                COALESCE(q2.numeric_value, 0) - COALESCE(q1.numeric_value, 0) AS numeric_value,
                'Q2' AS period_type,
                q1.report_year
            FROM reported_periods q1
            JOIN reported_periods q2 ON 
                q1.normalized_concept_name = q2.normalized_concept_name AND
                q1.report_year = q2.report_year AND
                q1.period_type = 'Q1' AND
                q2.period_type = 'Q2YTD'
        ),

        derived_q3 AS (
            SELECT 
                q2.normalized_concept_name,
                DATE(q2.report_year::text || '-07-01') AS period_start,
                DATE(q2.report_year::text || '-10-01') AS period_end,
                COALESCE(q3.numeric_value, 0) - COALESCE(q2.numeric_value, 0) AS numeric_value,
                'Q3' AS period_type,
                q2.report_year
            FROM reported_periods q2
            JOIN reported_periods q3 ON 
                q2.normalized_concept_name = q3.normalized_concept_name AND
                q2.report_year = q3.report_year AND
                q2.period_type = 'Q2YTD' AND
                q3.period_type = 'Q3YTD'
        ),

        derived_q4 AS (
            SELECT 
                q3.normalized_concept_name,
                DATE(q3.report_year::text || '-10-01') AS period_start,
                DATE((q3.report_year+1)::text || '-01-01') AS period_end,
                COALESCE(annual.numeric_value, 0) - COALESCE(q3.numeric_value, 0) AS numeric_value,
                'Q4' AS period_type,
                q3.report_year
            FROM reported_periods q3
            JOIN reported_periods annual ON 
                q3.normalized_concept_name = annual.normalized_concept_name AND
                q3.report_year = annual.report_year AND
                q3.period_type = 'Q3YTD' AND
                annual.period_type = 'Annual'
        ),

        combined_data AS (
            SELECT * FROM reported_periods
            UNION ALL SELECT * FROM derived_q2
            UNION ALL SELECT * FROM derived_q3
            UNION ALL SELECT * FROM derived_q4
        )

        SELECT 
            period_start,
            period_end,
            period_type,
            report_year,
            normalized_concept_name,
            numeric_value
        FROM combined_data
		where period_type in ('Q1', 'Q2', 'Q3', 'Q4')
        ORDER BY report_year, period_type, normalized_concept_name
    $sql$, view_name, view_name, p_ticker_symbol, p_exchange_code, p_ticker_symbol, p_exchange_code);

    IF p_debug THEN
        RAISE NOTICE 'Generated SQL: %', dynamic_sql;
    END IF;

    EXECUTE dynamic_sql;

    RAISE NOTICE 'View company.% created (no pivot)', view_name;
END;
$$ LANGUAGE plpgsql;
