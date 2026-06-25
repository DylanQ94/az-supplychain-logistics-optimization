
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'SupplyChainServerless2026!';
GO

SELECT name
FROM sys.symmetric_keys
WHERE name = '##MS_DatabaseMasterKey##';

CREATE DATABASE SCOPED CREDENTIAL SynapseWorkspaceManagedIdentity
WITH IDENTITY = 'Managed Identity';
GO

SELECT name
FROM sys.database_scoped_credentials
WHERE name = 'SynapseWorkspaceManagedIdentity';