{{ config(schema = 'core', materialized = 'ephemeral')}}

SELECT
    primary_key AS waste_asset_key,
    source_id,
    source_name,
    country,
    waste_type,
    reporting_year,
    reporting_month,
    emissions_quantity,
    waste_generated,
    (emissions_quantity/nullif(waste_generated, 0)) AS asset_intensity_ratio,
    CASE WHEN asset_intensity_ratio IS NOT NULL THEN 'CO2e' ELSE NULL END AS intensity_uom
FROM {{ ref('waste_emissions_unioned')}}