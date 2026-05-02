SELECT
    source_id,
    source_name,
    iso3_country AS country,
    CASE 
        WHEN subsector = 'domestic-wastewater-treatment-and-discharge' THEN 'domestic-wastewater'
        ELSE NULL 
        END AS waste_type,
    start_time:: DATE AS start_date,
    end_time:: DATE AS end_date,
    emissions_quantity:: FLOAT,
    CASE
        WHEN emissions_factor_units = 't of CO2e_20yr per population served or population equivalent' THEN 'MT CO2e'
        ELSE emissions_factor_units END AS emissions_uom,
    activity AS waste_generated,
    CASE
        WHEN activity_units = 'population served or population equivalent' THEN 'population'
        ELSE activity_units END AS waste_uom,
    {{ dbt_utils.generate_surrogate_key(['source_id', 'country', 'waste_type']) }} AS primary_key
FROM
    {{ source('waste_emissions', 'domestic_wastewater_emissions')}}
