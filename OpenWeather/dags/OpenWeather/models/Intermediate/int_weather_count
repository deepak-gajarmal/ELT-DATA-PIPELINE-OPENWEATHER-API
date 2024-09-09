

with weather_condition_count as (
    select
        name,
        to_date(dt) as dt,  
        weather_main,
        count(*) as weather_condition_count  
    from {{ ref('base_weather_info') }}
    group by name, to_date(dt), weather_main
)

select * from weather_condition_count
