{{ config(schema = 'marts', materialized = 'table')}}

SELECT 
    d.source_name,
    d.country,
    d.waste_type,
    SUM(f.emissions_MT_CO2e) AS total_emissions_mtco2e,
    SUM(f.waste_quantity_MT) AS tota_waste_mt
FROM {{ ref('fct_waste_assets') }} f
INNER JOIN {{ ref('dim_waste_sites') }} d
    ON f.waste_asset_key = d.waste_asset_key
GROUP BY 1, 2, 3
ORDER BY 4 DESC, 5 DESC