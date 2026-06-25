USE supply_chain_analytics_db;
GO

CREATE ROLE supply_chain_serving_reader;
GO

SELECT name
FROM sys.database_principals
WHERE name = 'supply_chain_serving_reader';

GRANT SELECT ON SCHEMA::serving TO supply_chain_serving_reader;
GO

GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::SynapseWorkspaceManagedIdentity
TO supply_chain_serving_reader;
GO

SELECT
    dp.name AS principal_name,
    dp.type_desc AS principal_type,
    pe.permission_name,
    pe.state_desc,
    pe.class_desc,
    CASE
        WHEN pe.class_desc = 'SCHEMA' THEN s.name
        WHEN pe.class_desc = 'DATABASE_SCOPED_CREDENTIAL' THEN dsc.name
        ELSE NULL
    END AS securable_name
FROM sys.database_permissions pe
INNER JOIN sys.database_principals dp
    ON pe.grantee_principal_id = dp.principal_id
LEFT JOIN sys.schemas s
    ON pe.class_desc = 'SCHEMA'
   AND pe.major_id = s.schema_id
LEFT JOIN sys.database_scoped_credentials dsc
    ON pe.class_desc = 'DATABASE_SCOPED_CREDENTIAL'
   AND pe.major_id = dsc.credential_id
WHERE dp.name = 'supply_chain_serving_reader'
ORDER BY
    pe.permission_name,
    pe.class_desc,
    securable_name;