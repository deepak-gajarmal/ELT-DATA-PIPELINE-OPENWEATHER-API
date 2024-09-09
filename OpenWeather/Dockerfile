FROM quay.io/astronomer/astro-runtime:12.1.0



# # Copy your dbt project into the airflow DAGs folder
COPY . /usr/local/airflow/dags/OpenWeather

# Copy the profiles.yml file from OpenWeather to the appropriate directory inside the container
COPY ./profiles.yml /usr/local/airflow/.dbt/profiles.yml





ENV SNOWFLAKE_USER=
ENV SNOWFLAKE_PASSWORD=
ENV SNOWFLAKE_ACCOUNT=
ENV SNOWFLAKE_WAREHOUSE=
ENV SNOWFLAKE_DATABASE=
ENV SNOWFLAKE_SCHEMA=
ENV API_KEY= 
