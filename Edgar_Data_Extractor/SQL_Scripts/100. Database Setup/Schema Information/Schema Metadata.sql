SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) AS total_size
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC;


SELECT 
    relname AS table_name, 
    n_live_tup AS row_count
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;


SELECT table_schema, table_name
FROM information_schema.views
WHERE table_schema NOT IN ('pg_catalog', 'information_schema');


SELECT routine_schema, routine_name, data_type
FROM information_schema.routines
WHERE routine_type='FUNCTION'
  AND routine_schema NOT IN ('pg_catalog', 'information_schema');
