from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
#from airflow_dbt.operators.dbt import DbtRunOperator, DbtTestOperator
from airflow_dbt.operators import DbtRunOperator, DbtTestOperator
from datetime import timedelta,datetime
import subprocess
import os
import snowflake.connector
import requests
import json
import pendulum


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to run your Python script
def run_python_script():
    API_KEY = os.getenv('API_KEY')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

    cities = [
    'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas',
    'San Jose', 'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 'San Francisco', 'Charlotte', 'Indianapolis', 
    'Seattle', 'Denver', 'Washington', 'Boston', 'El Paso', 'Nashville', 'Detroit', 'Las Vegas', 'Portland', 
    'Memphis', 'Oklahoma City', 'Louisville', 'Baltimore', 'Milwaukee', 'Albuquerque', 'Tucson', 'Fresno', 
    'Sacramento', 'Kansas City', 'Mesa', 'Atlanta', 'Omaha', 'Colorado Springs', 'Raleigh', 'Miami', 
    'Long Beach', 'Virginia Beach', 'Oakland', 'Minneapolis', 'Tulsa', 'Arlington', 'New Orleans', 'Wichita', 
    'Cleveland', 'Tampa', 'Bakersfield', 'Aurora', 'Honolulu', 'Anaheim', 'Santa Ana', 'Riverside', 'Corpus Christi', 
    'Lexington', 'Stockton', 'Henderson', 'Saint Paul', 'St. Louis', 'Cincinnati', 'Pittsburgh', 'Greensboro', 
    'Anchorage', 'Plano', 'Lincoln', 'Orlando', 'Irvine', 'Newark', 'Toledo', 'Durham', 'Chula Vista', 'Fort Wayne', 
    'Jersey City', 'St. Petersburg', 'Laredo', 'Madison', 'Chandler', 'Buffalo', 'Lubbock', 'Scottsdale', 'Reno', 
    'Glendale', 'Gilbert', 'Winston-Salem', 'North Las Vegas', 'Norfolk', 'Chesapeake', 'Garland', 'Irving', 
    'Hialeah', 'Fremont', 'Boise', 'Richmond', 'Baton Rouge', 'Spokane', 'Des Moines', 'Tacoma', 'San Bernardino',
    'Modesto', 'Fontana', 'Moreno Valley', 'Fayetteville', 'Huntington Beach', 'Glendale', 'Yonkers', 'Aurora', 
    'Montgomery', 'Columbus', 'Amarillo', 'Little Rock', 'Akron', 'Shreveport', 'Augusta', 'Grand Rapids', 'Mobile', 
    'Salt Lake City', 'Huntsville', 'Tallahassee', 'Grand Prairie', 'Overland Park', 'Knoxville', 'Port St. Lucie', 
    'Worcester', 'Brownsville', 'Tempe', 'Santa Clarita', 'Newport News', 'Cape Coral', 'Providence', 'Fort Lauderdale', 
    'Chattanooga', 'Rancho Cucamonga', 'Oceanside', 'Garden Grove', 'Sioux Falls', 'Ontario', 'Vancouver', 
    'McKinney', 'Elk Grove', 'Pembroke Pines', 'Salem', 'Eugene', 'Peoria', 'Corona', 'Cary', 'Springfield', 
    'Fort Collins', 'Jackson', 'Alexandria', 'Hayward', 'Lancaster', 'Lakewood', 'Clarksville', 'Palmdale', 
    'Salinas', 'Hollywood', 'Springfield', 'Macon', 'Kansas City', 'Sunnyvale', 'Pomona', 'Escondido', 
    'Killeen', 'Naperville', 'Joliet', 'Bellevue', 'Rockford', 'Savannah', 'Paterson', 'Torrance', 'Bridgeport', 
    'McAllen', 'Mesquite', 'Syracuse', 'Midland', 'Pasadena', 'Murfreesboro', 'Miramar', 'Dayton', 'Thornton', 
    'Roseville', 'Denton', 'West Valley City', 'Warren', 'Surprise', 'Carrollton', 'Westminster', 'Charleston', 
    'Olathe', 'Sterling Heights', 'Cedar Rapids', 'Visalia', 'Coral Springs', 'New Haven', 'Stamford', 
    'Concord', 'Kent', 'Santa Clara', 'Elizabeth', 'Round Rock', 'Thousand Oaks', 'Lafayette', 'Athens', 
    'Topeka', 'Simi Valley', 'Fargo', 'Norman', 'Columbia', 'Abilene'
]
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cursor = conn.cursor()

    for city in cities:
        params = {'q': city, 'appid': API_KEY, 'units': 'metric'}
        response = requests.get('http://api.openweathermap.org/data/2.5/weather', params=params)

        if response.status_code == 200:
            weather_data = response.json()
            id = weather_data.get('id')
            name = weather_data.get('name')
            coord_lon = weather_data['coord'].get('lon')
            coord_lat = weather_data['coord'].get('lat')
            weather_id = weather_data['weather'][0].get('id')
            weather_main = weather_data['weather'][0].get('main')
            weather_description = weather_data['weather'][0].get('description')
            weather_icon = weather_data['weather'][0].get('icon')
            base = weather_data.get('base')
            temp = weather_data['main'].get('temp')
            feels_like = weather_data['main'].get('feels_like')
            temp_min = weather_data['main'].get('temp_min')
            temp_max = weather_data['main'].get('temp_max')
            pressure = weather_data['main'].get('pressure')
            humidity = weather_data['main'].get('humidity')
            visibility = weather_data.get('visibility')
            wind_speed = weather_data['wind'].get('speed')
            wind_deg = weather_data['wind'].get('deg')
            wind_gust = weather_data['wind'].get('gust', None)  # Optional field
            clouds_all = weather_data['clouds'].get('all')
            rain_1h = weather_data['rain'].get('1h', None) if 'rain' in weather_data else None
            rain_3h = weather_data['rain'].get('3h', None) if 'rain' in weather_data else None
            snow_1h = weather_data['snow'].get('1h', None) if 'snow' in weather_data else None
            snow_3h = weather_data['snow'].get('3h', None) if 'snow' in weather_data else None
            dt = weather_data.get('dt')
            sys_type = weather_data['sys'].get('type')
            sys_id = weather_data['sys'].get('id')
            sys_country = weather_data['sys'].get('country')
            sys_sunrise = weather_data['sys'].get('sunrise')
            sys_sunset = weather_data['sys'].get('sunset')
            timezone = weather_data.get('timezone')
            timestamp = datetime.now()

            insert_query = f"""
            INSERT INTO stg_weather_data (
                id, name, coord_lon, coord_lat, weather_id, weather_main, weather_description, weather_icon, base, temp, 
                feels_like, temp_min, temp_max, pressure, humidity, visibility, wind_speed, wind_deg, wind_gust, clouds_all, 
                rain_1h, rain_3h, snow_1h, snow_3h, dt, sys_type, sys_id, sys_country, sys_sunrise, sys_sunset, timezone, timestamp
            ) 
            VALUES (
                {id}, '{name}', {coord_lon}, {coord_lat}, {weather_id}, '{weather_main}', '{weather_description}', '{weather_icon}', 
                '{base}', {temp}, {feels_like}, {temp_min}, {temp_max}, {pressure}, {humidity}, {visibility}, {wind_speed}, 
                {wind_deg}, {wind_gust}, {clouds_all}, {rain_1h}, {rain_3h}, {snow_1h}, {snow_3h}, {dt}, {sys_type}, {sys_id}, 
                '{sys_country}', {sys_sunrise}, {sys_sunset}, {timezone}, '{timestamp}'
            )
            """
            insert_query = insert_query.replace('None', 'NULL')
            cursor.execute(insert_query)
    cursor.close()
    conn.close()


# Initialize the DAG
with DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Orchestrate Python and dbt models for weather data',
    schedule_interval='0 3,12,15,21 * * *',  # Schedule the DAG to run daily
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_api_script = PythonOperator(
        task_id='run_api_script',
        python_callable=run_python_script
    )

    # Task 2: Run dbt models (transform the data in Snowflake)
    dbt_run = DbtRunOperator(
        task_id='dbt_run',
        dir='/usr/local/airflow/dags/OpenWeather',  # Path to your dbt project
        profiles_dir='/usr/local/airflow/.dbt/'  # Path to profiles.yml
    )

    # Task 3: Run dbt tests (optional)
    # dbt_test = DbtTestOperator(
    #     task_id='dbt_test',
    #     dir='/Users/deepak/Data_Pipeline_Project/OpenWeather',
    #     profiles_dir='/Users/deepak/.dbt'
    # )

    # Task dependencies: run the Python script first, then dbt run, then dbt test
    run_api_script>>dbt_run 