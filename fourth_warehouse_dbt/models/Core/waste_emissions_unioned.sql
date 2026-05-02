{{ config(schema = 'core', materialized = 'table')}}

WITH unioned_data AS(
SELECT * FROM {{ ref('stg_solid_waste')}}
UNION ALL
SELECT * FROM {{ ref('stg_domestic_wastewater')}}
UNION ALL
SELECT * FROM {{ ref('stg_industrial_wastewater')}})

SELECT 
    primary_key,
    source_id,
    source_name,
    country,
    waste_type,
    extract(YEAR from start_date) AS reporting_year,
    date_format(start_date, 'MMM') AS reporting_month,
    emissions_quantity,
    emissions_uom,
    waste_generated,
    waste_uom
FROM unioned_data

