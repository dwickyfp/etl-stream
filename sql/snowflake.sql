-- 1. Use a high-privilege role to create objects
USE ROLE SECURITYADMIN;

-----------------------------------------------------------
-- STEP 1: Create the Role
-----------------------------------------------------------
CREATE ROLE IF NOT EXISTS ETL_ROLE
    COMMENT = 'Role for ETL processes with full DDL/DML rights in DEVELOPMENT';

-----------------------------------------------------------
-- STEP 2: Grant Database Privileges
-----------------------------------------------------------
-- Grant usage on the specific database
GRANT USAGE ON DATABASE DEVELOPMENT TO ROLE ETL_ROLE;

-- Allow the role to create new schemas in this database
-- (The role will automatically be the OWNER of any schema it creates)
GRANT CREATE SCHEMA ON DATABASE DEVELOPMENT TO ROLE ETL_ROLE;

-----------------------------------------------------------
-- STEP 3: Grant Schema-Level Privileges (For EXISTING Schemas)
-----------------------------------------------------------
-- Note: If you want this role to work in the 'PUBLIC' schema or other 
-- existing schemas, run these. (Repeat for other specific schemas if needed)

GRANT USAGE ON SCHEMA DEVELOPMENT.PUBLIC TO ROLE ETL_ROLE;
GRANT CREATE TABLE ON SCHEMA DEVELOPMENT.PUBLIC TO ROLE ETL_ROLE;
GRANT CREATE VIEW ON SCHEMA DEVELOPMENT.PUBLIC TO ROLE ETL_ROLE;
GRANT CREATE TASK ON SCHEMA DEVELOPMENT.PUBLIC TO ROLE ETL_ROLE;
GRANT CREATE PROCEDURE ON SCHEMA DEVELOPMENT.PUBLIC TO ROLE ETL_ROLE;
GRANT CREATE FUNCTION ON SCHEMA DEVELOPMENT.PUBLIC TO ROLE ETL_ROLE;
GRANT CREATE FILE FORMAT ON SCHEMA DEVELOPMENT.PUBLIC TO ROLE ETL_ROLE;
GRANT CREATE STAGE ON SCHEMA DEVELOPMENT.PUBLIC TO ROLE ETL_ROLE;
GRANT CREATE SEQUENCE ON SCHEMA DEVELOPMENT.PUBLIC TO ROLE ETL_ROLE;
GRANT CREATE STREAM ON SCHEMA DEVELOPMENT.PUBLIC TO ROLE ETL_ROLE;
GRANT CREATE PIPE ON SCHEMA DEVELOPMENT.PUBLIC TO ROLE ETL_ROLE;

-- Grant data access on all current tables/views in PUBLIC
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA DEVELOPMENT.PUBLIC TO ROLE ETL_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA DEVELOPMENT.PUBLIC TO ROLE ETL_ROLE;

-----------------------------------------------------------
-- STEP 4: Configure Future Grants (For NEW Schemas/Objects)
-----------------------------------------------------------
-- This ensures that if tables are created by others (or in future schemas), 
-- this role automatically inherits rights.

-- OPTION A: Apply to all future schemas in the database
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE DEVELOPMENT TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN DATABASE DEVELOPMENT TO ROLE ETL_ROLE;
GRANT SELECT ON FUTURE VIEWS IN DATABASE DEVELOPMENT TO ROLE ETL_ROLE;

-----------------------------------------------------------
-- STEP 5: Task Execution Privileges
-----------------------------------------------------------
-- To execute/resume tasks, the role needs this account-level privilege
USE ROLE ACCOUNTADMIN; -- Switch to ACCOUNTADMIN for this specific grant
GRANT EXECUTE TASK ON ACCOUNT TO ROLE ETL_ROLE;

-- (Optional) If you use Serverless tasks, you might need:
-- GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE ETL_ROLE;

-----------------------------------------------------------
-- STEP 6: Warehouse Access (Required to run queries)
-----------------------------------------------------------
-- Replace 'COMPUTE_WH' with your specific warehouse name
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ETL_ROLE;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE ETL_ROLE; -- Optional: allows starting/suspending WH

-----------------------------------------------------------
-- STEP 7: Create the User
-----------------------------------------------------------
USE ROLE SECURITYADMIN;

CREATE USER IF NOT EXISTS ETL_USER
    PASSWORD = 'YourStrongPassword123!' -- Change this immediately
    DEFAULT_ROLE = ETL_ROLE
    DEFAULT_WAREHOUSE = COMPUTE_WH      -- Set their default warehouse
    COMMENT = 'Service user for ETL pipelines'
    MUST_CHANGE_PASSWORD = FALSE;       -- Set TRUE if a human will log in first

-----------------------------------------------------------
-- STEP 8: Final Assignment
-----------------------------------------------------------
GRANT ROLE ETL_ROLE TO USER ETL_USER;
GRANT ROLE ETL_ROLE TO ROLE SYSADMIN; -- Best practice: allows SYSADMIN to manage this role

USE ROLE SECURITYADMIN;

-- Set the public key for the user
ALTER USER ETL_USER SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0';

-- Verify the key fingerprint matches
DESC USER ETL_USER;
-- Look at the 'RSA_PUBLIC_KEY_FP' column in the output