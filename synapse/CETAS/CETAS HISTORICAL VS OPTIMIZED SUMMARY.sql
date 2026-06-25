--- CREATING CETAS HISTORICAL VS OPTIMIZED SUMMARY ---

USE supply_chain_analytics_db;
GO

IF EXISTS (
    SELECT 1
    FROM sys.external_tables t
    INNER JOIN sys.schemas s
        ON t.schema_id = s.schema_id
    WHERE s.name = 'materialized'
      AND t.name = 'historical_vs_optimized_summary'
)
BEGIN
    DROP EXTERNAL TABLE materialized.historical_vs_optimized_summary;
END;
GO

CREATE EXTERNAL TABLE materialized.historical_vs_optimized_summary
WITH (
    LOCATION = 'synapse_materialized/supply_chain_logistics/historical_vs_optimized_summary/',
    DATA_SOURCE = ds_supply_chain,
    FILE_FORMAT = ParquetFileFormat
)
AS
SELECT
    comparison_scope,
    historical_order_count,
    orders_with_valid_options,
    orders_with_optimized_assignment,
    orders_unassigned_under_constraints,
    orders_compared,
    orders_without_historical_cost,
    assignment_coverage_percentage,
    historical_total_cost,
    optimized_total_cost,
    estimated_savings,
    savings_percentage
FROM optimization.vw_fact_cost_comparison;
GO

--- CETAS VALIDATION ---
SELECT * FROM materialized.historical_vs_optimized_summary;