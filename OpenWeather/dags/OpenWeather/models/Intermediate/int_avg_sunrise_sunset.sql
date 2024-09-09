

with avg_sunrise_sunset as (
    select
        name,
        to_timestamp(avg(date_part('epoch_second', "Sunrise"))) as avg_sunrise_time,  -- Convert to epoch, avg, then 
        to_timestamp(avg(date_part('epoch_second', "Sunset"))) as avg_sunset_time     -- Calculate the average sunset time
    from {{ ref('base_weather_info') }}
    group by name
)

select * from avg_sunrise_sunset
