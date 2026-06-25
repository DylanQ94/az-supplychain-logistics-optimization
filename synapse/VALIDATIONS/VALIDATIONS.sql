---- VIEWS VALIDATIONS ----

SELECT
    s.name AS schema_name,
    v.name AS view_name
FROM sys.views v
INNER JOIN sys.schemas s
    ON v.schema_id = s.schema_id
WHERE s.name IN ('gold', 'optimization')
ORDER BY
    s.name,
    v.name;
GO

---- EXTERNAL TABLES VALIDATIONS ----

SELECT 
    s.name AS schema_name,
    t.name AS table_name
FROM sys.external_tables t
INNER JOIN sys.schemas s
    ON t.schema_id = s.schema_id
WHERE s.name = 'external_data'
ORDER BY t.name;

---- MATERIALIZED EXTERNAL TABLES VALIDATIONS ----

SELECT
    s.name AS schema_name,
    t.name AS external_table_name,
    ds.name AS data_source_name,
    ff.name AS file_format_name,
    t.location
FROM sys.external_tables t
INNER JOIN sys.schemas s
    ON t.schema_id = s.schema_id
INNER JOIN sys.external_data_sources ds
    ON t.data_source_id = ds.data_source_id
INNER JOIN sys.external_file_formats ff
    ON t.file_format_id = ff.file_format_id
WHERE s.name = 'materialized'
ORDER BY t.name;

---- OTHER VALIDATIONS ----

USE supply_chain_analytics_db;
GO

SELECT
    s.name AS schema_name,
    v.name AS object_name,
    'VIEW' AS object_type
FROM sys.views v
INNER JOIN sys.schemas s
    ON v.schema_id = s.schema_id
WHERE s.name IN ('gold', 'optimization', 'serving')
UNION ALL
SELECT
    s.name AS schema_name,
    t.name AS object_name,
    'EXTERNAL TABLE' AS object_type
FROM sys.external_tables t
INNER JOIN sys.schemas s
    ON t.schema_id = s.schema_id
WHERE s.name = 'materialized'
ORDER BY
    schema_name,
    object_type,
    object_name;
