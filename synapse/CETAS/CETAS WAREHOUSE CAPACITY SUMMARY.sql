--- CREATING CETAS WAREHOUSE CAPACITY SUMMARY ---

IF EXISTS (
    SELECT 1
    FROM sys.external_tables t
    INNER JOIN sys.schemas s
        ON t.schema_id = s.schema_id
    WHERE s.name = 'materialized'
      AND t.name = 'warehouse_capacity_summary'
)
BEGIN
    DROP EXTERNAL TABLE materialized.warehouse_capacity_summary;
END;
GO

CREATE EXTERNAL TABLE materialized.warehouse_capacity_summary
WITH (
    LOCATION = 'synapse_materialized/supply_chain_logistics/warehouse_capacity_summary/',
    DATA_SOURCE = ds_supply_chain,
    FILE_FORMAT = ParquetFileFormat
)
AS
SELECT
    plant_code,
    COUNT_BIG(*) AS active_date_count,
    SUM(candidate_order_count) AS candidate_order_count,
    SUM(optimized_order_count) AS optimized_order_count,
    SUM(unassigned_candidate_order_count) AS unassigned_candidate_order_count,
    SUM(assigned_elsewhere_candidate_order_count) AS assigned_elsewhere_candidate_order_count,
    SUM(optimized_total_unit_quantity) AS optimized_total_unit_quantity,
    SUM(optimized_total_weight) AS optimized_total_weight,
    AVG(daily_order_capacity) AS average_daily_order_capacity,
    AVG(optimized_capacity_usage_ratio) AS average_optimized_capacity_usage_ratio,
    AVG(candidate_capacity_pressure_ratio) AS average_candidate_capacity_pressure_ratio,
    SUM(remaining_order_capacity) AS total_remaining_order_capacity,
    SUM(CASE WHEN is_capacity_exceeded = 1 THEN 1 ELSE 0 END) AS capacity_exceeded_day_count
FROM optimization.vw_fact_optimized_capacity_usage
GROUP BY
    plant_code;
GO

SELECT * FROM materialized.warehouse_capacity_summary
ORDER BY average_candidate_capacity_pressure_ratio DESC;