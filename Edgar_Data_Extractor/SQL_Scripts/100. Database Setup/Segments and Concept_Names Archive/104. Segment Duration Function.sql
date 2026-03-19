drop view if exists company.segment_duration;
CREATE VIEW company.segment_duration AS	
	WITH Annuals_and_Quarters as (
	SELECT sh.concept_name
			, sh.parent_qname
			, sh.member_name
			, sh.period_start
			, sh.period_end
			, sh.numeric_value
			, sh.filing_date
			, sh.company_id
			, case when extract(month from sh.period_end)=4 and sh.period_end-sh.period_start between 85 and 95 then 'Q1'
				when extract(month from sh.period_end)=7 and sh.period_end-sh.period_start between 85 and 95 then 'Q2'
				when extract(month from sh.period_end)=10 and sh.period_end-sh.period_start between 85 and 95 then 'Q3'
				when extract(month from sh.period_end)=7 and sh.period_end-sh.period_start between 175 and 185 then 'YTDQ2'
				when extract(month from sh.period_end)=10 and sh.period_end-sh.period_start between 265 and 275 then 'YTDQ3'
				when extract(month from sh.period_end)=1 and sh.period_end-sh.period_start between 360 and 370 then 'Annual'
				end as duration_type
			, numeric_value as n1
			, numeric_value as n2
	FROM company.segment_history sh
	-- order by sh.concept_name, sh.parent_qname, sh.member_name, sh.period_start, sh.period_end, sh.numeric_value
	)
	,
	-- CTE to calculate sums for Q1-Q3 for each concept and year
	quarterly_sums AS (
		SELECT 
			concept_name
			, parent_qname
			, member_name
			, extract(year from period_start) as report_year
			, sum(numeric_value) AS q1_q3_sum
			, company_id
		FROM Annuals_and_Quarters
		where duration_type like 'Q%'
		GROUP BY concept_name, parent_qname, member_name, extract(year from period_start), company_id
		-- order by concept_name, parent_qname, member_name,extract(year from period_start)
	)
	,
	-- CTE to filter just annual data
	annual_data AS (
		SELECT distinct concept_name
		, parent_qname
		, member_name
		, duration_type
		, extract(year from period_start) as report_year
		, numeric_value
		, company_id
		, filing_date
		FROM Annuals_and_Quarters 
		WHERE duration_type = 'Annual'

		UNION
		
		SELECT
			distinct
			aq1.concept_name
			, aq1.parent_qname
			, aq1.member_name
			, 'Annual_Derived' as duration_type
			, extract(year from aq1.period_start) as report_year
			, aq1.numeric_value + aq2.numeric_value AS numeric_value
			, aq1.company_id
			, aq1.filing_date
		FROM Annuals_and_Quarters aq1
		JOIN Annuals_and_Quarters aq2 ON aq1.concept_name = aq2.concept_name 
								and aq1.parent_qname = aq2.parent_qname
								and aq1.member_name = aq2.member_name
							 	and extract(year from aq1.period_start) = extract(year from aq2.period_start)
		where aq1.duration_type = 'YTDQ3'
			and aq2.duration_type = 'Q4'
	)
	, 
	q2_final as (
	SELECT
		distinct
		aq1.concept_name
		, aq1.parent_qname
		, aq1.member_name
		, DATE(extract(year from aq1.period_start) || '-04-01') AS period_start
		, DATE(extract(year from aq2.period_start) || '-07-01') AS period_end
		, aq1.numeric_value - aq2.numeric_value AS numeric_value
		, aq1.company_id
		, aq1.filing_date
		, 'Q2' as duration_type
		, aq1.duration_type as aq1
		, aq2.duration_type as aq2
		, aq1.numeric_value as n1
		, aq2.numeric_value as n2
	FROM Annuals_and_Quarters aq1
	JOIN Annuals_and_Quarters aq2 ON aq1.concept_name = aq2.concept_name 
							and aq1.parent_qname = aq2.parent_qname
							and aq1.member_name = aq2.member_name
						 	and extract(year from aq1.period_start) = extract(year from aq2.period_start)
							and aq1.duration_type = 'YTDQ2'
							and aq2.duration_type = 'Q1'
	UNION

	SELECT
		concept_name
		, parent_qname
		, member_name
		, period_start
		, period_end
		, numeric_value
		, company_id
		, filing_date
		, duration_type
		, duration_type as aq1
		, duration_type as aq2
		, numeric_value as n1
		, numeric_value as n2
	FROM Annuals_and_Quarters 
	where duration_type = 'Q2'
						 )
	, 
	q3_final as (
	SELECT
		distinct
		aq1.concept_name
		, aq1.parent_qname
		, aq1.member_name
		, DATE(extract(year from aq1.period_start) || '-07-01') AS period_start
		, DATE(extract(year from aq2.period_start) || '-10-01') AS period_end
		, aq1.numeric_value - aq2.numeric_value AS numeric_value
		, aq1.company_id
		, aq1.filing_date
		, 'Q3' as duration_type
		, aq1.duration_type as aq1
		, aq2.duration_type as aq2
		, aq1.numeric_value as n1
		, aq2.numeric_value as n2
	FROM Annuals_and_Quarters aq1
	JOIN Annuals_and_Quarters aq2 ON aq1.concept_name = aq2.concept_name 
							and aq1.parent_qname = aq2.parent_qname
							and aq1.member_name = aq2.member_name
						 	and extract(year from aq1.period_start) = extract(year from aq2.period_start)
							and aq1.duration_type = 'YTDQ3'
							and aq2.duration_type = 'YTDQ2'
	UNION

	SELECT
		concept_name
		, parent_qname
		, member_name
		, period_start
		, period_end
		, numeric_value
		, company_id
		, filing_date
		, duration_type
		, duration_type as aq1
		, duration_type as aq2
		, numeric_value as n1
		, numeric_value as n2
	FROM Annuals_and_Quarters 
	where duration_type = 'Q3'
						 )
						 ,
	derived_q4 as (
	SELECT
		distinct
		a.concept_name
		, a.parent_qname
		, a.member_name
		, DATE(a.report_year::text || '-10-01') AS period_start
		, DATE((a.report_year+1)::text || '-01-01') AS period_end
		, a.numeric_value - COALESCE(q.q1_q3_sum, 0) AS numeric_value
		, a.company_id
		, a.filing_date
		, 'Q4' as duration_type
		, 'Annual' as aq1
		, 'cumulative' as aq2
		, a.numeric_value as n1
		, COALESCE(q.q1_q3_sum, 0) as n2
	FROM annual_data a
	JOIN quarterly_sums q ON a.concept_name = q.concept_name 
							and a.parent_qname = q.parent_qname
							and a.member_name = q.member_name
						 and a.report_year = q.report_year

	UNION

	SELECT
		distinct
		aq1.concept_name
		, aq1.parent_qname
		, aq1.member_name
		, DATE(extract(year from aq1.period_start) || '-10-01') AS period_start
		, DATE(extract(year from aq2.period_start)+1 || '-01-01') AS period_end
		, aq1.numeric_value - aq2.numeric_value AS numeric_value
		, aq1.company_id
		, aq1.filing_date
		, 'Q4' as duration_type
		, aq1.duration_type as aq1
		, aq2.duration_type as aq2
		, aq1.numeric_value as n1
		, aq2.numeric_value as n2
	FROM Annuals_and_Quarters aq1
	JOIN Annuals_and_Quarters aq2 ON aq1.concept_name = aq2.concept_name 
							and aq1.parent_qname = aq2.parent_qname
							and aq1.member_name = aq2.member_name
						 	and extract(year from aq1.period_start) = extract(year from aq2.period_start)
							and aq1.duration_type = 'Annual'
							and aq2.duration_type = 'YTDQ3'
						 )
	,
	combined_data AS (
	SELECT concept_name, parent_qname, member_name, period_start, period_end, numeric_value, company_id, filing_date, duration_type, 
	duration_type as aq1, duration_type as aq2, n1, n2
	FROM Annuals_and_Quarters
	where duration_type = 'Q1'
	-- or duration_type = 'Annual'

	UNION
	SELECT concept_name, parent_qname, member_name, period_start, period_end, numeric_value, company_id, filing_date, duration_type, aq1, aq2, n1, n2
	FROM q2_final

	UNION
	SELECT concept_name, parent_qname, member_name, period_start, period_end, numeric_value, company_id, filing_date, duration_type, aq1, aq2, n1, n2
	FROM q3_final
	
	UNION 
	SELECT concept_name, parent_qname, member_name, period_start, period_end, numeric_value, company_id, filing_date, duration_type, aq1, aq2, n1, n2
	FROM derived_q4),
	filing_dates_ranked as (
	SELECT 
	    COALESCE(icnm.normalized_concept_name, ccnm.normalized_concept_name, cd.concept_name) AS concept_name,
	    cd.parent_qname,
	    cd.member_name,
	    cd.period_start,
	    cd.period_end,
	    cd.numeric_value,
	    cd.filing_date,
	    ROW_NUMBER() OVER (
	        PARTITION BY 
	            cd.concept_name,
	            cd.parent_qname,
	            cd.member_name,
	            cd.period_start,
	            cd.period_end
	        ORDER BY
	            CASE WHEN cd.aq1 = cd.aq2 THEN 0 ELSE 1 END,              -- Priority 1: exact match
	            cd.filing_date DESC,                                     -- Priority 2: latest filing
	            CASE WHEN cd.aq2 ILIKE 'ytd%' THEN 0 ELSE 1 END,         -- Priority 3: prefer 'ytd'
	            CASE WHEN cd.aq2 = 'cumulative' THEN 1 ELSE 0 END        -- Priority 4: push 'cumulative' to bottom
	    ) AS rnk,
	    cd.aq1,
	    cd.aq2,
	    cd.n1,
	    cd.n2
	FROM combined_data cd
	LEFT JOIN company.IS_concept_name_mapping icnm
	    ON cd.concept_name = icnm.original_concept_name
	   AND cd.company_id = icnm.company_id
	LEFT JOIN company.CF_concept_name_mapping ccnm
	    ON cd.concept_name = ccnm.original_concept_name
	   AND cd.company_id = ccnm.company_id
	)
	select concept_name
		, parent_qname
		, member_name
		, period_start
		, period_end
		, numeric_value
		, filing_date
		, rnk
		, aq1
		, aq2
		, n1
		, n2
	from filing_dates_ranked
	where rnk = 1
	order by concept_name, parent_qname, member_name, period_start, period_end, numeric_value;
	

select * from company.segment_history


select *
from company.segment_history
where concept_name = 'RevenueFromContractWithCustomerExcludingAssessedTax'
and member_name = 'CH, MexicanIMMSAUnitMember, OperatingSegmentsMember'
and period_start = '2020-01-01'


