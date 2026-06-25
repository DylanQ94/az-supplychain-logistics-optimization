--- CREATING CETAS ROUTE COST SUMMARY ---

IF EXISTS (
    SELECT 1
    FROM sys.external_tables t
    INNER JOIN sys.schemas s
        ON t.schema_id = s.schema_id
    WHERE s.name = 'materialized'
      AND t.name = 'route_cost_summary'
)
BEGIN
    DROP EXTERNAL TABLE materialized.route_cost_summary;
END;
GO

CREATE EXTERNAL TABLE materialized.route_cost_summary
WITH (
    LOCATION = 'synapse_materialized/supply_chain_logistics/route_cost_summary/',
    DATA_SOURCE = ds_supply_chain,
    FILE_FORMAT = ParquetFileFormat
)
AS
SELECT
    optimized_plant_code,
    optimized_orig_port,
    dest_port,
    optimized_carrier,
    optimized_transport_mode,
    service_level,
    COUNT_BIG(*) AS optimized_order_count,
    SUM(unit_quant) AS total_unit_quantity,
    SUM(weight) AS total_weight,
    AVG(optimized_tpt_day_count) AS average_optimized_tpt_day_count,
    SUM(historical_total_cost) AS historical_total_cost,
    SUM(optimized_total_cost) AS optimized_total_cost,
    SUM(estimated_savings) AS estimated_savings,
    CASE
        WHEN SUM(historical_total_cost) = 0 THEN NULL
        ELSE SUM(estimated_savings) / SUM(historical_total_cost)
    END AS savings_percentage
FROM optimization.vw_fact_optimized_order_assignment
GROUP BY
    optimized_plant_code,
    optimized_orig_port,
    dest_port,
    optimized_carrier,
    optimized_transport_mode,
    service_level;
GO

SELECT TOP 20 *
FROM materialized.route_cost_summary
ORDER BY estimated_savings DESC;