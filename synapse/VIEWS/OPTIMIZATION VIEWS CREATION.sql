CREATE OR ALTER VIEW optimization.vw_fact_cost_comparison AS
SELECT *
FROM OPENROWSET(
    BULK 'optimization/supply_chain_logistics/fact_cost_comparison/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS fact_cost_comparison;
GO

CREATE OR ALTER VIEW optimization.vw_fact_optimized_capacity_usage AS
SELECT *
FROM OPENROWSET(
    BULK 'optimization/supply_chain_logistics/fact_optimized_capacity_usage/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS fact_optimized_capacity_usage;
GO

CREATE OR ALTER VIEW optimization.vw_fact_optimized_order_assignment AS
SELECT *
FROM OPENROWSET(
    BULK 'optimization/supply_chain_logistics/fact_optimized_order_assignment/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS fact_optimized_order_assignment;
GO

CREATE OR ALTER VIEW optimization.vw_fact_unassigned_order_diagnostic AS
SELECT *
FROM OPENROWSET(
    BULK 'optimization/supply_chain_logistics/fact_unassigned_order_diagnostic/',
    DATA_SOURCE = 'ds_supply_chain',
    FORMAT = 'DELTA'
) AS fact_unassigned_order_diagnostic;
GO