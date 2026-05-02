SELECT
    source_id,
    source_name,
    iso3_country AS country,
    CASE 
        WHEN subsector = 'solid-waste-disposal' THEN 'solid-waste'
        ELSE subsector 
        END AS waste_type,
    try_to_date(start_time, 'MM-dd-yyyy') AS start_date,
    end_time:: DATE AS end_date,
    emissions_quantity:: FLOAT,
    CASE
        WHEN emissions_factor_units = 't of CO2e_20yr per t of waste' THEN 'MT CO2e'
        ELSE emissions_factor_units END AS emissions_uom,
    activity AS waste_generated,
    CASE
        WHEN activity_units = 't of waste' THEN 'tonnes'
        ELSE activity_units END AS waste_uom,
    {{ dbt_utils.generate_surrogate_key(['source_id', 'source_name', 'country', 'waste_type', 'start_date', 'end_date']) }} AS primary_key
FROM
    {{ source('waste_emissions', 'solid_waste_emissions')}}
