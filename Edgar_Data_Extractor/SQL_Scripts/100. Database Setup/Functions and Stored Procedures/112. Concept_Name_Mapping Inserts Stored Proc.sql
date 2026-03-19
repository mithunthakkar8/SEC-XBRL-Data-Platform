CREATE OR REPLACE PROCEDURE company.insert_concept_name_mappings(
    p_ticker_symbol VARCHAR,
    p_exchange_code VARCHAR
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_company_id UUID;
    v_insert_count INT := 0;
    v_update_count INT := 0;
    v_error_message TEXT;
    v_error_detail TEXT;
    v_error_hint TEXT;
    v_error_context TEXT;
BEGIN
    -- Get company ID first to validate the company exists
    SELECT company_id INTO v_company_id
    FROM xbrl.company
    WHERE ticker_symbol = p_ticker_symbol
    AND exchange_code = p_exchange_code;
    
    IF v_company_id IS NULL THEN
        RAISE EXCEPTION 'Company not found with ticker % and exchange code %', p_ticker_symbol, p_exchange_code;
    END IF;
    
    -- Income Statement (IS) concepts
    INSERT INTO company.IS_concept_name_mapping (original_concept_name, ticker_symbol, exchange_code, company_id)
    SELECT DISTINCT 
        cpt.concept_name,
        c.ticker_symbol,
        c.exchange_code,
        c.company_id
    FROM xbrl.reported_fact rf 
    JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
    JOIN xbrl.filing f ON rf.filing_id = f.filing_id
    JOIN xbrl.company c ON c.company_id = f.company_id
    JOIN xbrl.concept_relationship cr ON cr.child_name = cpt.concept_name AND cr.child_ns = cpt.namespace
    WHERE cr.role_id IN (
        SELECT role_id
        FROM xbrl.link_role
        WHERE role_uri ~* 'ConsolidatedStatements?Of(Income|Operations|Earnings)'
    )
    AND rf.has_segment = FALSE
    AND c.ticker_symbol = p_ticker_symbol
    AND c.exchange_code = p_exchange_code
    ON CONFLICT (original_concept_name, company_id) DO NOTHING;
    
    GET DIAGNOSTICS v_insert_count = ROW_COUNT;
    RAISE NOTICE 'Inserted % records into IS_concept_name_mapping', v_insert_count;

    -- Update normalized_concept_name for IS concepts
    UPDATE company.IS_concept_name_mapping
    SET normalized_concept_name = original_concept_name
    WHERE company_id = v_company_id
    AND (normalized_concept_name IS NULL OR normalized_concept_name != original_concept_name);
    
    GET DIAGNOSTICS v_update_count = ROW_COUNT;
    RAISE NOTICE 'Updated % records in IS_concept_name_mapping', v_update_count;

    -- Balance Sheet (BS) concepts
    INSERT INTO company.BS_concept_name_mapping (original_concept_name, ticker_symbol, exchange_code, company_id)
    SELECT DISTINCT
        cpt.concept_name,
        c.ticker_symbol,
        c.exchange_code,
        c.company_id
    FROM xbrl.reported_fact rf 
    JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
    JOIN xbrl.filing f ON rf.filing_id = f.filing_id
    JOIN xbrl.company c ON c.company_id = f.company_id
    JOIN xbrl.concept_relationship cr ON cr.child_name = cpt.concept_name AND cr.child_ns = cpt.namespace
    WHERE cr.role_id IN (
        SELECT role_id
        FROM xbrl.link_role
        WHERE role_uri ~* 'BalanceSheets?'
    )
    AND rf.has_segment = FALSE
    AND c.ticker_symbol = p_ticker_symbol
    AND c.exchange_code = p_exchange_code
    ON CONFLICT (original_concept_name, company_id) DO NOTHING;

    GET DIAGNOSTICS v_insert_count = ROW_COUNT;
    RAISE NOTICE 'Inserted % records into BS_concept_name_mapping', v_insert_count;

    -- Update normalized_concept_name for BS concepts
    UPDATE company.BS_concept_name_mapping
    SET normalized_concept_name = original_concept_name
    WHERE company_id = v_company_id
    AND (normalized_concept_name IS NULL OR normalized_concept_name != original_concept_name);
    
    GET DIAGNOSTICS v_update_count = ROW_COUNT;
    RAISE NOTICE 'Updated % records in BS_concept_name_mapping', v_update_count;

    -- Cash Flow (CF) concepts
    INSERT INTO company.CF_concept_name_mapping (original_concept_name, ticker_symbol, exchange_code, company_id)
    SELECT DISTINCT 
        cpt.concept_name,
        c.ticker_symbol,
        c.exchange_code,
        c.company_id
    FROM xbrl.reported_fact rf 
    JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
    JOIN xbrl.filing f ON rf.filing_id = f.filing_id
    JOIN xbrl.company c ON c.company_id = f.company_id
    JOIN xbrl.concept_relationship cr ON cr.child_name = cpt.concept_name AND cr.child_ns = cpt.namespace
    WHERE cr.role_id IN (
        SELECT role_id
        FROM xbrl.link_role
        WHERE role_uri ~* 'CONSOLIDATEDSTATEMENTSOFCASHFLOWS'
    )
    AND rf.has_segment = FALSE
    AND c.ticker_symbol = p_ticker_symbol
    AND c.exchange_code = p_exchange_code
    ON CONFLICT (original_concept_name, company_id) DO NOTHING;
    
    GET DIAGNOSTICS v_insert_count = ROW_COUNT;
    RAISE NOTICE 'Inserted % records into CF_concept_name_mapping', v_insert_count;

    -- Update normalized_concept_name for CF concepts
    UPDATE company.CF_concept_name_mapping
    SET normalized_concept_name = original_concept_name
    WHERE company_id = v_company_id
    AND (normalized_concept_name IS NULL OR normalized_concept_name != original_concept_name);
    
    GET DIAGNOSTICS v_update_count = ROW_COUNT;
    RAISE NOTICE 'Updated % records in CF_concept_name_mapping', v_update_count;
    
    RAISE NOTICE 'Successfully processed concept mappings for % (%)', p_ticker_symbol, p_exchange_code;
    
EXCEPTION
    WHEN OTHERS THEN
        -- ROLLBACK is automatic when an error occurs in a procedure
        GET STACKED DIAGNOSTICS 
            v_error_message = MESSAGE_TEXT,
            v_error_detail = PG_EXCEPTION_DETAIL,
            v_error_hint = PG_EXCEPTION_HINT,
            v_error_context = PG_EXCEPTION_CONTEXT;
        
        RAISE EXCEPTION 'Failed to process concept mappings for % (%)
Error: %
Detail: %
Hint: %
Context: %', 
            p_ticker_symbol, p_exchange_code, 
            v_error_message, v_error_detail, v_error_hint, v_error_context;
END;
$$;