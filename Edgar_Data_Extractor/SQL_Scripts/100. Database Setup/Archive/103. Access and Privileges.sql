REVOKE CONNECT ON DATABASE finhub FROM PUBLIC;

GRANT CONNECT ON DATABASE finhub TO finhub_admin;
GRANT USAGE, CREATE ON SCHEMA public TO finhub_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO finhub_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO finhub_admin;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO finhub_admin;

REVOKE CONNECT ON DATABASE finhub FROM postgres;
REVOKE ALL PRIVILEGES ON SCHEMA public FROM postgres;

REVOKE SELECT ON pg_stat_activity FROM PUBLIC;
REVOKE SELECT ON pg_stat_activity FROM postgres;



SELECT pid, usename, client_addr, state 
FROM pg_stat_activity 
WHERE datname = 'finhub';





