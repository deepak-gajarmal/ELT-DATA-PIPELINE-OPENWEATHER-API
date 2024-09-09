with base_data as 
(
select ID,
       Name,
       COORD_LON,
       COORD_LAT,
       WEATHER_MAIN,
       WEATHER_DESCRIPTION,
       TEMP,
       TEMP_MIN,
       TEMP_MAX,
       FEELS_LIKE,
       PRESSURE,
       HUMIDITY,
       VISIBILITY,
       WIND_SPEED,
       WIND_DEG,
       to_date(to_timestamp(dt)) as "DT",
       to_timestamp(sys_sunrise) as "Sunrise",
       to_timestamp(sys_sunset) as "Sunset",
       TIMESTAMP
from {{ ref('stg_weather_info') }}
)

select * from base_data