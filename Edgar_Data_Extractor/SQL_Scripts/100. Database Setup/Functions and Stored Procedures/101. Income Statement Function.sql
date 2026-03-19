-- Drop existing function if it exists to allow replacement
DROP FUNCTION IF EXISTS company.create_income_statement_view;

-- Create or replace the function with three parameters:
-- p_ticker_symbol: The stock ticker symbol to create the view for (text)
-- p_exchange_code: The exchange code where the stock is listed (char(4))
-- p_debug: Boolean flag to enable debug output (defaults to false)
CREATE OR REPLACE FUNCTION company.create_income_statement_view(
    p_ticker_symbol text,
    p_exchange_code char(4),
    p_debug boolean DEFAULT false
) RETURNS text AS $$
DECLARE
    dynamic_sql text;          -- Stores the dynamic SQL to create the view
    column_list text;          -- Stores the list of columns for the pivot
    view_name text;            -- Stores the name of the view to create
    concept_count integer;     -- Count of concept mappings found
    data_count integer;        -- Count of data rows found
    view_check integer;        -- Count of rows in the created view
    company_id_val UUID;       -- Stores the company ID from the ticker symbol and exchange
    role_uri_pattern text := 'ConsolidatedStatements?Of(Income|Operations|Earnings)'; -- Pattern to match income statement roles
BEGIN
    -- Input validation - ensure ticker symbol and exchange code are not empty
    IF p_ticker_symbol IS NULL OR trim(p_ticker_symbol) = '' THEN
        RETURN 'Error: Ticker symbol cannot be empty';
    END IF;
    
    IF p_exchange_code IS NULL OR trim(p_exchange_code) = '' THEN
        RETURN 'Error: Exchange code cannot be empty';
    END IF;

    -- Create the view name by combining schema, ticker symbol and exchange code
    view_name := format('company.%s_%s_income_statement', lower(p_ticker_symbol), lower(p_exchange_code));

    -- Debug output if enabled
    IF p_debug THEN
        RAISE NOTICE 'Starting view creation for ticker: % on exchange: %', p_ticker_symbol, p_exchange_code;
        RAISE NOTICE 'View name will be: %', view_name;
    END IF;

    -- Verify the company exists in the database
    BEGIN
        -- Get company ID for the ticker symbol and exchange code
        SELECT company_id INTO company_id_val 
        FROM xbrl.company 
        WHERE ticker_symbol = p_ticker_symbol
        AND exchange_code = p_exchange_code;
        
        -- If no company found, return error
        IF NOT FOUND THEN
            RETURN format('Error: Company with ticker %s on exchange %s not found', p_ticker_symbol, p_exchange_code);
        END IF;
        
        -- Debug output if enabled
        IF p_debug THEN
            RAISE NOTICE 'Found company with ID: %s', company_id_val;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        -- Return any errors that occur during company lookup
        RETURN format('Error checking company: %s', SQLERRM);
    END;

    -- Check if concept mappings exist for this ticker and exchange
    BEGIN
        -- Count concept mappings for the ticker and exchange
        SELECT COUNT(*) INTO concept_count
        FROM company.IS_concept_name_mapping
        WHERE ticker_symbol = p_ticker_symbol
        AND exchange_code = p_exchange_code;
        
        -- Debug output if enabled
        IF p_debug THEN
            RAISE NOTICE 'Found % concept mappings for ticker % on exchange %', concept_count, p_ticker_symbol, p_exchange_code;
        END IF;
        
        -- If no concepts found, return error
        IF concept_count = 0 THEN
            RETURN format('Error: No income statement concepts mapped for ticker %s on exchange %s', p_ticker_symbol, p_exchange_code);
        END IF;
    EXCEPTION WHEN OTHERS THEN
        -- Return any errors that occur during concept mapping check
        RETURN format('Error checking concept mappings: %s', SQLERRM);
    END;

    -- Check if actual data exists for this company
    BEGIN
        -- Count data rows for the company
        SELECT COUNT(*) INTO data_count
        FROM xbrl.reported_fact rf
        JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
        JOIN company.IS_concept_name_mapping cnm ON cnm.original_concept_name = cpt.concept_name
        JOIN xbrl.filing f ON rf.filing_id = f.filing_id
        WHERE f.company_id = company_id_val
        AND rf.has_segment = false;
        
        -- Debug output if enabled
        IF p_debug THEN
            RAISE NOTICE 'Found % raw data rows for ticker % on exchange %', data_count, p_ticker_symbol, p_exchange_code;
        END IF;
        
        -- If no data found, return error
        IF data_count = 0 THEN
            RETURN format('Error: No financial data found for ticker %s on exchange %s', p_ticker_symbol, p_exchange_code);
        END IF;
    EXCEPTION WHEN OTHERS THEN
        -- Return any errors that occur during data check
        RETURN format('Error checking data existence: %s', SQLERRM);
    END;

    -- Drop existing view if it exists (with CASCADE to handle dependencies)
    BEGIN
        EXECUTE format('DROP VIEW IF EXISTS %s CASCADE', view_name);
        IF p_debug THEN
            RAISE NOTICE 'Dropped existing view if it existed with cascade';
        END IF;
    EXCEPTION WHEN OTHERS THEN
        -- Return any errors that occur during view drop
        RETURN format('Error dropping existing view: %s', SQLERRM);
    END;

    -- Build the column list for the pivot table with error handling
    BEGIN
        -- Generate a comma-separated list of column definitions
        SELECT string_agg(
            format(
                -- Each column will be a MAX(CASE WHEN...) expression
                'COALESCE(MAX(CASE WHEN normalized_concept_name = %L THEN normalized_numeric_value END), 0) AS %s',
                normalized_concept_name,
                -- Convert camelCase to snake_case and limit to 63 chars to comply with postgres norms of being able to work with 63 char length column
                lower(substring(regexp_replace(normalized_concept_name, '([a-z])([A-Z])', '\1_\2', 'g'), 1, 63))
            ),
            ', '
        ) INTO column_list
        FROM (
            -- Get distinct normalized concept names for income statement items
            SELECT DISTINCT cnm.normalized_concept_name
            FROM xbrl.concept_relationship cr
            JOIN xbrl.link_role lr ON cr.role_id = lr.role_id
            JOIN xbrl.concept cpt ON cr.child_name = cpt.concept_name AND cr.child_ns = cpt.namespace
            JOIN company.IS_concept_name_mapping cnm ON cnm.original_concept_name = cpt.concept_name
            WHERE lr.role_uri ~* role_uri_pattern
            AND cnm.ticker_symbol = p_ticker_symbol
            AND cnm.exchange_code = p_exchange_code
        ) AS distinct_concepts;
        
        -- Debug output if enabled
        IF p_debug THEN
            RAISE NOTICE 'Generated column list with % columns', 
                (SELECT array_length(string_to_array(column_list, ','), 1));
            -- Uncomment to see full column list in debug output:
            -- RAISE NOTICE 'Column list: %', column_list;
        END IF;
        
        -- If no columns generated, return error
        IF column_list IS NULL THEN
            RETURN 'Error: No valid columns found for income statement view';
        END IF;
    EXCEPTION WHEN OTHERS THEN
        -- Return any errors that occur during column list generation
        RETURN format('Error generating column list: %s', SQLERRM);
    END;

    -- Build and execute the dynamic SQL to create the view
    BEGIN
        -- Construct the full view creation SQL
        dynamic_sql := format('
            CREATE VIEW %s AS
            -- CTE to get the latest filing dates for each concept and period
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
            -- CTE to get raw data with normalized values and period types
            raw_data AS (
                SELECT distinct
                    cnm.normalized_concept_name,
                    cp.period_start,
                    cp.period_end,
                    -- Normalize values based on balance type (debit/credit)
                    CASE WHEN ca.balance_type = ''debit'' THEN -rf.numeric_value ELSE rf.numeric_value END AS normalized_numeric_value,
                    -- Classify periods as Annual, Q1-Q4, or Other
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
            ),
            -- CTE to calculate sums for Q1-Q3 for each concept and year
            quarterly_sums AS (
                SELECT 
                    normalized_concept_name,
                    report_year,
                    SUM(CASE WHEN period IN (''Q1'',''Q2'',''Q3'') 
                        THEN normalized_numeric_value ELSE 0 END) AS q1_q3_sum
                FROM raw_data
                GROUP BY normalized_concept_name, report_year
            ),
            -- CTE to filter just annual data
            annual_data AS (
                SELECT * FROM raw_data WHERE period = ''Annual''
            ),
            -- CTE to derive Q4 data by subtracting Q1-Q3 sums from annual values
            derived_q4 AS (
                SELECT 
                    a.normalized_concept_name,
                    DATE(a.report_year::text || ''-10-01'') AS period_start,
                    DATE((a.report_year+1)::text || ''-01-01'') AS period_end,
                    CASE 
                        -- Handle EPS (Earnings Per Share) differently
                        WHEN a.normalized_concept_name iLIKE ''%%PerShare%%'' THEN
                            NULL
                        -- Handle shares differently (annualize then average)
                        WHEN a.normalized_concept_name ILIKE ''%%Shares%%'' THEN 
                            (a.normalized_numeric_value*12.0 - COALESCE(q.q1_q3_sum*3, 0))*1.0/3.0
                        -- Standard calculation for other items
                        ELSE
                            a.normalized_numeric_value - COALESCE(q.q1_q3_sum, 0)
                    END AS normalized_numeric_value,
                    ''Q4'' AS quarter,
                    a.report_year
                FROM annual_data a
                LEFT JOIN quarterly_sums q ON a.normalized_concept_name = q.normalized_concept_name 
                                         AND a.report_year = q.report_year
            ),
            -- Combine all data (reported Q1-Q3, derived Q4, and annual)
            combined_data AS (
                SELECT normalized_concept_name, period_start, period_end, normalized_numeric_value
                FROM raw_data
                WHERE period IN (''Q1'',''Q2'',''Q3'')
                
                UNION ALL
                
                SELECT normalized_concept_name, period_start, period_end, normalized_numeric_value
                FROM derived_q4
                
                UNION ALL
                
                SELECT normalized_concept_name, period_start, period_end, normalized_numeric_value
                FROM raw_data
                WHERE period = ''Annual''
            )
            -- Final pivot of all data grouped by period
            SELECT 
                period_start,
                period_end,
                %s  -- This is where the generated column list gets inserted
            FROM combined_data
            GROUP BY period_start, period_end
            ORDER BY period_start, period_end
        ', view_name, role_uri_pattern, p_ticker_symbol, p_exchange_code, p_ticker_symbol, p_exchange_code, column_list);

        -- Debug output if enabled
        IF p_debug THEN
            RAISE NOTICE 'Executing dynamic SQL: %', dynamic_sql;
        END IF;

        -- Execute the dynamic SQL to create the view
        EXECUTE dynamic_sql;

        -- Verify the view was actually created
        PERFORM 1 FROM information_schema.views 
        WHERE table_schema = 'company' 
        AND table_name = lower(p_ticker_symbol) || '_' || lower(p_exchange_code) || '_income_statement';
        
        -- If view not found after creation, return error
        IF NOT FOUND THEN
            RETURN 'Error: View creation failed - check permissions or SQL syntax';
        END IF;

        -- Get row count from the newly created view
        EXECUTE format('SELECT COUNT(*) FROM %s', view_name) INTO view_check;
        
        -- Debug output if enabled
        IF p_debug THEN
            RAISE NOTICE 'View created successfully with %s rows', view_check;
        END IF;

        -- Return success message with view name and row count
        RETURN format('Successfully created view %s with %s rows', view_name, view_check);
    EXCEPTION WHEN OTHERS THEN
        -- Return any errors that occur during view creation
        RETURN format('Error creating view: %s', SQLERRM);
    END;
END;
$$ LANGUAGE plpgsql;

