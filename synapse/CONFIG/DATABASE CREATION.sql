--- DATABASE CREATION ---
CREATE DATABASE supply_chain_analytics_db;

SELECT name
FROM sys.databases
WHERE name = 'supply_chain_analytics_db';

--- SCHEMAS CREATION ---
CREATE SCHEMA gold;
GO

CREATE SCHEMA optimization;
GO

CREATE SCHEMA materialized;
GO

CREATE SCHEMA serving;
GO

--- SCHEMAS VALIDATIONS ---
SELECT 
    name AS schema_name
FROM sys.schemas
WHERE name IN (
    'gold',
    'optimization',
    'materialized',
    'serving'
)
ORDER BY name;

