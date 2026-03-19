delete from company.segment_history;

-- Insert the data from your query
INSERT INTO company.segment_history (
    company_id,
    concept_name,
    parent_qname,
    member_name,
    period_start,
    period_end,
    instant_date,
    numeric_value,
    balance_type,
    filing_date,
	period_type
)
WITH segments_full_history AS (
    SELECT 
        cpt.concept_name,
        f.filing_date,
        cr.parent_qname,
        string_agg(distinct m.member_name, ', ' order by m.member_name) as member_name,
        cp.period_start,
        cp.period_end,
        cp.instant_date,
        rf.numeric_value,
        ca.balance_type,
        cny.company_id,
        max(f.filing_date) over (partition by cpt.concept_name, cr.parent_qname, string_agg(distinct m.member_name, ', ' order by m.member_name), cp.period_start, cp.period_end) as filing_date_max,
		cp.period_type
    FROM xbrl.reported_fact rf 
    JOIN xbrl.concept cpt ON rf.concept_id = cpt.concept_id
    JOIN xbrl.concept_relationship cr ON cr.child_name = cpt.concept_name AND cr.child_ns = cpt.namespace
    JOIN xbrl.concept_attribute ca ON cpt.concept_id = ca.concept_id
    JOIN xbrl.filing f ON rf.filing_id = f.filing_id
    JOIN xbrl.company cny ON f.company_id = cny.company_id
    JOIN xbrl.context c ON rf.context_id = c.context_id
    JOIN xbrl.context_period cp ON c.period_id = cp.context_period_id
    JOIN xbrl.context_dimension_members cdm ON c.context_id = cdm.context_id
    JOIN xbrl.dimension_member m ON cdm.member_id = m.member_id
    WHERE cr.role_id IN (
        SELECT role_id
        FROM xbrl.link_role
        WHERE role_uri ~* 'segment'
    )
    AND rf.has_segment = TRUE
    GROUP BY rf.fact_id, cpt.concept_name, cr.parent_qname, cp.period_start, cp.period_end, 
             rf.numeric_value, ca.balance_type, f.filing_date, cp.instant_date, cny.company_id, cp.period_type
)
SELECT distinct
    company_id,
    concept_name,
    parent_qname,
    member_name,
    period_start,
    period_end,
    instant_date,
    numeric_value,
    balance_type,
    filing_date,
	period_type
FROM segments_full_history
WHERE filing_date = filing_date_max
AND numeric_value IS NOT NULL
