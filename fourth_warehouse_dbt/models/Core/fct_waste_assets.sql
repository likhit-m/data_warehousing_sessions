{{ config(schema = 'core', 
  materialized = 'incremental', 
  unique_key='waste_asset_key',
  incremental_strategy = 'merge')}}

SELECT 
    waste_asset_key,
    reporting_year,
    reporting_month,
    emissions_quantity AS emissions_MT_CO2e,
    waste_generated AS waste_quantity_MT,
    asset_intensity_ratio AS intensity_ration_CO2e,
    current_timestamp AS dbt_updated_at
FROM {{ ref('waste_intensity_ratios')}}

{% if is_incremental() %}
  WHERE reporting_year > (SELECT MAX(reporting_year) FROM {{ this }})
{% endif %}