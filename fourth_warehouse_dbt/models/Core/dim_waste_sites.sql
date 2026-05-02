{{ config(schema = 'core', materialized = 'table')}}

SELECT 
    waste_asset_key,
    source_id,
    source_name,
    country,
    waste_type
FROM {{ ref('waste_intensity_ratios')}}
QUALIFY ROW_NUMBER() OVER(PARTITION BY source_id ORDER BY reporting_year DESC) = 1
