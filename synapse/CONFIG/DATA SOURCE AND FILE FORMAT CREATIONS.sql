-- CREATING EXTERNAL DATA SOURCE
CREATE EXTERNAL DATA SOURCE ds_supply_chain
WITH (
    LOCATION = 'abfss://supply-chain@sasupplychainz.dfs.core.windows.net',
    CREDENTIAL = SynapseWorkspaceManagedIdentity
);
GO

-- VALIDATING THE DATA SOURCE CREATED
SELECT
    name,
    location
FROM sys.external_data_sources
WHERE name = 'ds_supply_chain';

-- CREATING THE EXTERNAL FILE FORMTAT FOR PARQUET FILES
CREATE EXTERNAL FILE FORMAT ParquetFileFormat
WITH (
    FORMAT_TYPE = PARQUET
);
GO

-- VALIDATING THE EXTERNAL FILE FORMTAT CREATED
SELECT name, format_type
FROM sys.external_file_formats
WHERE name = 'ParquetFileFormat';

