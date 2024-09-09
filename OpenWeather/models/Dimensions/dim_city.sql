

with city_details as (
    select distinct
        name,
        coord_lon as longitude,
        coord_lat as latitude
    from {{ ref('base_weather_info') }}
)

select * from city_details
