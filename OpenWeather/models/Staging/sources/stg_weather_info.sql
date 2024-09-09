with source as
(
    select *
    from {{source('WEATHER','stg_weather_data')}}
)

select * from source