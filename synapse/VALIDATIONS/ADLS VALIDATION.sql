USE supply_chain_analytics_db;
GO

SELECT TOP 10
    *
FROM OPENROWSET(
    BULK 'gold/supply_chain_logistics/fact_order_shipment/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS fact_order_shipment;

SELECT
    COUNT(*) AS total_rows
FROM OPENROWSET(
    BULK 'gold/supply_chain_logistics/fact_order_shipment/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS fact_order_shipment;

SELECT TOP 10
    order_id
FROM OPENROWSET(
    BULK 'gold/supply_chain_logistics/fact_order_shipment/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS fact_order_shipment;