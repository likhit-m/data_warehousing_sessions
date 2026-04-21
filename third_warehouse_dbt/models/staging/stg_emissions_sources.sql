WITH emissions_data_three_years AS (
SELECT 
    source_id,
    source_name,
    iso3_country AS country_code,
    sector,
    subsector,
    start_time:: DATE AS start_date,
    end_time:: DATE AS end_date,
    emissions_quantity,
    emissions_factor_units AS UOM
FROM
    {{ source('climate_trace_steel', 'emissions_sources') }}
WHERE extract(year from start_date) >= 2023 and extract(year from end_date) < 2026)

SELECT *
FROM emissions_data_three_years

