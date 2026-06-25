CREATE OR ALTER VIEW gold.vw_fact_order_shipment AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/supply_chain_logistics/fact_order_shipment/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS fact_order_shipment;
GO

CREATE OR ALTER VIEW gold.vw_fact_freight_cost_option AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/supply_chain_logistics/fact_freight_cost_option/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS fact_freight_cost_option;
GO

CREATE OR ALTER VIEW gold.vw_fact_warehouse_capacity_usage AS
SELECT *
FROM OPENROWSET(
    BULK 'gold/supply_chain_logistics/fact_warehouse_capacity_usage/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS fact_warehouse_capacity_usage;
GO