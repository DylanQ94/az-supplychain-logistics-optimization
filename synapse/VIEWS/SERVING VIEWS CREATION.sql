/* ============================================================
   SERVING VIEWS CREATION
   ============================================================ */

USE supply_chain_analytics_db;
GO

--- SERVING DIMENSIONS VIEWS ---

CREATE OR ALTER VIEW serving.vw_dim_date AS
SELECT
    date_key,
    order_date,
    calendar_year,
    calendar_quarter,
    month_number,
    month_name,
    year_month
FROM gold.vw_dim_date;
GO

CREATE OR ALTER VIEW serving.vw_dim_product AS
SELECT
    product_id
FROM gold.vw_dim_product;
GO

CREATE OR ALTER VIEW serving.vw_dim_customer AS
SELECT
    customer,
    is_vmi_customer
FROM gold.vw_dim_customer;
GO

CREATE OR ALTER VIEW serving.vw_dim_plant AS
SELECT
    plant_code,
    cost_per_unit,
    daily_order_capacity
FROM gold.vw_dim_plant;
GO

CREATE OR ALTER VIEW serving.vw_dim_port AS
SELECT
    port,
    is_origin_port,
    is_destination_port
FROM gold.vw_dim_port;
GO

CREATE OR ALTER VIEW serving.vw_dim_carrier AS
SELECT
    carrier,
    carrier_type
FROM gold.vw_dim_carrier;
GO

CREATE OR ALTER VIEW serving.vw_dim_transport_mode AS
SELECT
    transport_mode
FROM gold.vw_dim_transport_mode;
GO

CREATE OR ALTER VIEW serving.vw_dim_service_level AS
SELECT
    service_level
FROM gold.vw_dim_service_level;
GO

--- SERVING FACTS VIEWS ---
CREATE OR ALTER VIEW serving.vw_fact_order_shipment AS
SELECT
    order_id,
    date_key,
    order_date,
    customer,
    product_id,
    plant_code,
    orig_port,
    dest_port,
    carrier,
    service_level,
    unit_quant,
    weight,
    tpt_day_count,
    ship_ahead_day_count,
    ship_late_day_count,
    historical_freight_cost,
    historical_warehouse_handling_cost,
    historical_total_logistics_cost
FROM gold.vw_fact_order_shipment;
GO

CREATE OR ALTER VIEW serving.vw_fact_optimized_order_assignment AS
SELECT
    order_id,
    date_key,
    order_date,
    customer,
    product_id,
    dest_port,
    service_level,
    unit_quant,
    weight,
    historical_plant_code,
    historical_orig_port,
    historical_carrier,
    historical_tpt_day_count,
    optimized_plant_code,
    optimized_orig_port,
    optimized_carrier,
    optimized_transport_mode,
    optimized_carrier_type,
    optimized_tpt_day_count,
    optimized_option_rank,
    is_lowest_cost_option,
    is_vmi_customer,
    is_candidate_plant_vmi_restricted,
    optimized_warehouse_daily_order_capacity,
    historical_freight_cost,
    historical_warehouse_handling_cost,
    historical_total_cost,
    optimized_freight_cost,
    optimized_warehouse_handling_cost,
    optimized_total_cost,
    estimated_savings,
    savings_percentage
FROM optimization.vw_fact_optimized_order_assignment;
GO

CREATE OR ALTER VIEW serving.vw_fact_unassigned_order_diagnostic AS
SELECT
    order_id,
    date_key,
    order_date,
    customer,
    product_id,
    dest_port,
    service_level,
    unit_quant,
    weight,
    historical_plant_code,
    historical_orig_port,
    historical_carrier,
    valid_option_count,
    candidate_plant_count,
    candidate_plants,
    full_candidate_plant_count,
    available_candidate_plant_count,
    minimum_available_option_cost,
    constraint_diagnostic_reason
FROM optimization.vw_fact_unassigned_order_diagnostic;
GO

--- SERVING MATERIALIZED CETAS VIEWS ---
CREATE OR ALTER VIEW serving.vw_historical_vs_optimized_summary AS
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
FROM materialized.historical_vs_optimized_summary;
GO

CREATE OR ALTER VIEW serving.vw_customer_savings_summary AS
SELECT
    customer,
    optimized_order_count,
    historical_total_cost,
    optimized_total_cost,
    estimated_savings,
    savings_percentage,
    average_order_savings_percentage
FROM materialized.customer_savings_summary;
GO

CREATE OR ALTER VIEW serving.vw_carrier_cost_summary AS
SELECT
    optimized_carrier,
    optimized_carrier_type,
    optimized_transport_mode,
    optimized_order_count,
    total_unit_quantity,
    total_weight,
    historical_total_cost,
    optimized_total_cost,
    estimated_savings,
    savings_percentage,
    average_optimized_order_cost
FROM materialized.carrier_cost_summary;
GO

CREATE OR ALTER VIEW serving.vw_warehouse_capacity_summary AS
SELECT
    plant_code,
    active_date_count,
    candidate_order_count,
    optimized_order_count,
    unassigned_candidate_order_count,
    assigned_elsewhere_candidate_order_count,
    optimized_total_unit_quantity,
    optimized_total_weight,
    average_daily_order_capacity,
    average_optimized_capacity_usage_ratio,
    average_candidate_capacity_pressure_ratio,
    total_remaining_order_capacity,
    capacity_exceeded_day_count
FROM materialized.warehouse_capacity_summary;
GO

CREATE OR ALTER VIEW serving.vw_route_cost_summary AS
SELECT
    optimized_plant_code,
    optimized_orig_port,
    dest_port,
    optimized_carrier,
    optimized_transport_mode,
    service_level,
    optimized_order_count,
    total_unit_quantity,
    total_weight,
    average_optimized_tpt_day_count,
    historical_total_cost,
    optimized_total_cost,
    estimated_savings,
    savings_percentage
FROM materialized.route_cost_summary;
GO
