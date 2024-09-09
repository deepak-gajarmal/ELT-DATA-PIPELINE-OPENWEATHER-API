
with weather_conditions as (
    select distinct
        ID as weather_id,
        name,
        weather_main as condition_name,
        weather_description as condition_description
    from {{ ref('base_weather_info') }}
)

select * from weather_conditions
