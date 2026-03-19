select cr.parent_name, cr.child_name, numeric_value, period_start, period_end, member_name, dimension_name, concept_name, cr.presentation_path
from xbrl.reported_fact rf
join xbrl.concept c
on rf.concept_id = c.concept_id
join xbrl.concept_relationship cr
on c.concept_name = cr.child_name
and c.namespace = cr.child_ns
join xbrl.context cxt
on cxt.context_id = rf.context_id
join xbrl.context_period cp
on cxt.period_id = cp.context_period_id
join xbrl.context_dimension_members cdm 
ON cxt.context_id = cdm.context_id
join xbrl.dimension_declaration dd
on cdm.dimension_id = dd.dimension_id
join xbrl.dimension_member m 
ON cdm.member_id = m.member_id
-- where m.member_name = 'CopperMember'
where cp.period_type = 'duration'
and rf.numeric_value = 1284100000
and cp.period_start = '2024-07-01'
and rf.has_segment = True
order by period_start desc;




select * from xbrl.dimension_member
where member_name IN (
    'OperatingSegmentsMember',
    'MolybdenumMember',
    'MexicanOpenPitMember'
  ) 


WITH context_with_all_three AS (
  SELECT context_id
  FROM xbrl.context_dimension_members
  WHERE member_name IN (
    'USGAAPOperatingSegmentsMember',
    'SCCOMolybdenumMember',
    'SCCOMexicanOpenPitMember'
  )
  GROUP BY context_id
  HAVING COUNT(DISTINCT member_name) = 3
)

SELECT rf.*, cdm.dimension_name, cdm.member_name
FROM xbrl.reported_fact rf
JOIN context_with_all_three ctx3 ON rf.context_id = ctx3.context_id
JOIN xbrl.context_dimension_members cdm ON rf.context_id = cdm.context_id
