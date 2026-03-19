
-- Create or replace a function named 'create_user_if_not_exists'
-- Return VOID, meaning do not return any value
CREATE OR REPLACE FUNCTION create_user_if_not_exists(username TEXT, password TEXT) RETURNS VOID AS $$
BEGIN
    -- Check if a role (user) with the given username already exists in the PostgreSQL role catalog
    IF NOT EXISTS (
        -- Query the 'pg_roles' system catalog table to check for the existence of the role
        SELECT FROM pg_catalog.pg_roles 
        WHERE rolname = username -- Filter by the provided username
    ) THEN
        -- If the role does not exist, dynamically create the user using the EXECUTE statement
        -- The 'format()' function is used to safely construct the SQL command (to avoid SQL Injection)
        -- %I is used to safely escape the username (identifier)
        -- %L is used to safely escape the password (literal)
        EXECUTE format('CREATE USER %I WITH PASSWORD %L', username, password);
		RAISE NOTICE 'User "%" created successfully.', username;
	ELSE
		RAISE NOTICE 'User "%" already exists.', username;
    END IF;
END;
$$ LANGUAGE plpgsql; 

-- Call the function
SELECT create_user_if_not_exists('finhub_admin', 'pass@123');

ALTER ROLE "finhub_admin" WITH SUPERUSER;
ALTER ROLE "finhub_admin" WITH CREATEDB;
ALTER ROLE "finhub_admin" WITH CREATEROLE;


-- -- check user roles and privileges
-- SELECT rolname, rolsuper, rolcreaterole, rolcreatedb, rolcanlogin 
-- FROM pg_roles
-- where rolname = 'finhub_admin';

-- -- check which objects are owned by the role finhub_admin
-- SELECT relname AS object_name, relkind AS object_type 
-- FROM pg_class 
-- WHERE relowner = (SELECT oid FROM pg_roles WHERE rolname = 'finhub_admin');



-- DROP OWNED BY "finhub_admin";

-- drop role if exists "finhub_admin"


