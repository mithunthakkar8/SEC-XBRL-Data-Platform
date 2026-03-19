
DO $$ 
DECLARE
    metric_columns TEXT;
    dynamic_query TEXT;
BEGIN
    -- Step 1: Fetch metric types from the metrics table
    SELECT STRING_AGG(
        'COALESCE(MAX(CASE WHEN metric_type = ''' || metric_type || ''' THEN value END), 0) AS "' || metric_type || '"',
        ', '
    ) INTO metric_columns
    FROM (SELECT metric_type FROM alphascope.metric_types where financial_statement = 'income_statement'
	and metric_type IN ('Revenue', 'GAAP_Cost_of_Sales', 'Depreciation_And_Amortization', 'Interest_Expense')) AS subquery;
    
    -- Step 2: Construct the pivot query dynamically
    dynamic_query := 
        'DROP MATERIALIZED VIEW IF EXISTS alphascope.pivoted_financials;
		CREATE MATERIALIZED VIEW alphascope.pivoted_financials AS
        with IS_metric_details as (
		select cf.company_id
			, REGEXP_REPLACE(c.name, ''[^a-zA-Z0-9 ]'', '''') as company_name
			, fm.period_of_report
			, fm.metric_type
			, abs(fm.value) as value
			, cf.report_frequency
			, mt.financial_statement
		from alphascope.financial_metrics as fm
		join alphascope.company_filings as cf
		on fm.company_filing_id = cf.company_filing_id
		join alphascope.metric_types as mt
		on mt.metric_type = fm.metric_type
		join alphascope.company as c
		on cf.company_id = c.company_id
		where mt.financial_statement = ''income_statement'' 
		and fm.metric_type IN (''Revenue'', ''GAAP_Cost_of_Sales'', ''Depreciation_And_Amortization'', ''Interest_Expense'')),
		Nine_Month_Sums as (
		select company_id
			, to_date(extract(year from period_of_report)::TEXT || ''-09-30'', ''YYYY-MM-DD'') as period_of_report
			, metric_type
			, sum(value) as Nine_Month_Sum
		from IS_metric_details
		where report_frequency = ''Quarterly''
		group by company_id, metric_type, extract(year from period_of_report) ),
		quarterly_IS_ex_diluted_eps as (
		select ism.company_id
			, ism.company_name
			, ism.period_of_report
			, ism.metric_type
			, (case when ism.metric_type = ''Weighted_Average_Outstanding_Shares'' 
				and extract(month from ism.period_of_report) = 12 then ism.value*4 else
				ism.value end) - coalesce(lag(nms.Nine_Month_Sum) 
				over (partition by ism.company_id, ism.metric_type order by ism.period_of_report),0) as value
			, case when report_frequency = ''Annual'' then ''Quarterly'' else report_frequency end as frequency
		from IS_metric_details as ism
		left join Nine_Month_Sums as nms
		on ism.period_of_report = nms.period_of_report
		and ism.metric_type = nms.metric_type
		)
		SELECT 
            company_id,
            company_name,
            period_of_report, ' || metric_columns || '
        FROM quarterly_IS_ex_diluted_eps
        GROUP BY company_id, company_name, period_of_report
        ORDER BY period_of_report DESC';

    -- Step 3: Execute the dynamic query
    EXECUTE dynamic_query;
END $$;

select * from alphascope.pivoted_financials
where company_name = 'SOUTHERN COPPER CORP'