USE supply_chain_analytics_db;
GO

CREATE OR ALTER VIEW gold.vw_dim_product AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/supply_chain_logistics/dim_product/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS dim_product;
GO

CREATE OR ALTER VIEW gold.vw_dim_customer AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/supply_chain_logistics/dim_customer/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS dim_customer;
GO

CREATE OR ALTER VIEW gold.vw_dim_plant AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/supply_chain_logistics/dim_plant/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS dim_plant;
GO

CREATE OR ALTER VIEW gold.vw_dim_port AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/supply_chain_logistics/dim_port/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS dim_port;
GO

CREATE OR ALTER VIEW gold.vw_dim_carrier AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/supply_chain_logistics/dim_carrier/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS dim_carrier;
GO

CREATE OR ALTER VIEW gold.vw_dim_transport_mode AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/supply_chain_logistics/dim_transport_mode/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS dim_transport_mode;
GO

CREATE OR ALTER VIEW gold.vw_dim_service_level AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/supply_chain_logistics/dim_service_level/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS dim_service_level;
GO

CREATE OR ALTER VIEW gold.vw_dim_date AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/supply_chain_logistics/dim_date/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS dim_date;
GO