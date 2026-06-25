-- CREATING UNITY CATALOG --
CREATE CATALOG IF NOT EXISTS az_supplychain
COMMENT 'Unity Catalog for the Supply Chain Logistics Problem';

-- CREATING BRONZE SQUEMA --
CREATE SCHEMA IF NOT EXISTS az_supplychain.bronze
COMMENT 'Bronze layer with raw Delta tables from the supply chain logistics dataset';

-- CREATING SILVER SQUEMA --
CREATE SCHEMA IF NOT EXISTS az_supplychain.silver
COMMENT 'Silver layer with cleaned, typed and business-ready supply chain tables';

-- CREATING GOLD SQUEMA --
CREATE SCHEMA IF NOT EXISTS az_supplychain.gold
COMMENT 'Gold analytical model for Synapse and Power BI consumption';

-- CREATING OPTIMIZATION SQUEMA --
CREATE SCHEMA IF NOT EXISTS az_supplychain.optimization
COMMENT 'Optimization layer with cost-minimization outputs and comparison facts';

-- CREATING QUALITY SQUEMA --
CREATE SCHEMA IF NOT EXISTS az_supplychain.quality
COMMENT 'Data quality validation outputs for the Az-SupplyChain project';



-- CREATING VOLUME IN BRONZE SQUEMA --
CREATE EXTERNAL VOLUME IF NOT EXISTS az_supplychain.bronze.landing_files
LOCATION 'abfss://supply-chain@sasupplychainz.dfs.core.windows.net/landing/supply_chain_logistics/'
COMMENT 'Governed external volume for raw source files ingested from Figshare into Landing';