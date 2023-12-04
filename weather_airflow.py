from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.bash import BashOperator
import pendulum
from datetime import timedelta
import os
import openmeteo_requests
import requests_cache
from retry_requests import retry
import datetime

# API URL
url = "https://api.open-meteo.com/v1/forecast"
# Set your lat/long for Kiel or any other city
# Latitude / Longitude Finder:
# https://www.latlong.net/
params = {
	"latitude": 54.3213,
	"longitude": 10.1349,
	"hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "rain"],
	"timezone": "Europe/Berlin"
}

"""
The following DAG uses TaskFlow. It is a simplified version of Airflow with Python tasks in mind
https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
"""

# Setup the Open-Meteo API client with cache and retry on error
cwd = os.getcwd()
# Specify the relative path to the .cache directory
cache_directory = os.path.expanduser("~/.cache/openmeteo")
print(f"Cache Directory: {cache_directory}")
# Ensure that the cache directory exists
os.makedirs(cache_directory, exist_ok=True)
# Use the cache_directory variable when initializing CachedSession
cache_session = requests_cache.CachedSession(cache_directory, expire_after=3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)
# Get the response with the corrects params
responses = openmeteo.weather_api(url, params=params)

# Airflow default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=10),
    'retry_exponential_backoff':True,
}

@dag(
    # set schedule, e.g. daily, monthly, weekly
    'Weather_Airflow',
    default_args=default_args,
    schedule="@daily",
    start_date=pendulum.datetime(2023, 12, 1, tz="UTC"),
    # catchup is used to start the dag from the last sart date
    catchup=False,
    # use dags to parse the dags in your 
    tags=["weather"],
)

def weather():
    @task
    def check_weather_resp(responses):
        # Process first location. Add a for-loop for multiple locations or weather models
        response = responses[0]
        print(f"Coordinates {response.Latitude()}°E {response.Longitude()}°N")
        print(f"Elevation {response.Elevation()} m asl")
        print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
        print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    @task
    def create_dataframe(responses):
        import pandas as pd
        # Process hourly data. The order of variables needs to be the same as requested.
        response = responses[0]
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(2).ValuesAsNumpy()
        hourly_rain = hourly.Variables(3).ValuesAsNumpy()
        hourly_data = {"date": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s"),
            end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        )}
        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
        hourly_data["precipitation"] = hourly_precipitation
        hourly_data["rain"] = hourly_rain
        hourly_dataframe = pd.DataFrame(data = hourly_data)
        print(hourly_dataframe)
        return hourly_dataframe
    @task
    def save_plot(df):
        import matplotlib.pyplot as plt
        import seaborn as sns
        # Line plot for temperature over time
        plt.figure(figsize=(12, 6))
        sns.lineplot(x='date', y='temperature_2m', data=df)
        plt.title('Temperature Over Time')
        plt.xlabel('Date')
        plt.ylabel('Temperature (°C)')
        plt.xticks(rotation=45)
        # plt.show() not really feasible in Airflow
        # save image instead
        #plt.show()
        # Include the current datetime in the file name
        current_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"weather_plot_{current_datetime}.png"

        # Save the plot with the timestamped file name
        plt.savefig("/files/"+file_name)
        

    # check if API works using respose 
    check_weather_resp(responses = responses)
    # create and save dataframe to df
    df = create_dataframe(responses)
    # use df
    save_plot(df)

weather()