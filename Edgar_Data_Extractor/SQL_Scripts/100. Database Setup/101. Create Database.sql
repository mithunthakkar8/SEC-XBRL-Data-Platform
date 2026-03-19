-- Database: finhub

-- DROP DATABASE IF EXISTS "finhub";

CREATE DATABASE finhub
    WITH
    ENCODING = 'UTF8'
    LOCALE_PROVIDER = 'icu' -- icu helps PostgreSQL run consistently across all OS (Windows, Linux, macOS)
	ICU_LOCALE = 'en-US'
    TABLESPACE = pg_default
    CONNECTION LIMIT = 100
	TEMPLATE template0;