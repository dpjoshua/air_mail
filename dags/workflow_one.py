import os
import pandas as pd
import requests
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor
from airflow.exceptions import AirflowException  # Import AirflowException

# Constants
API_KEY = os.getenv("API_KEY")
DB_PATH = "/opt/airflow/dags/db"
TABLE_NAME = "merged_data"
SELECTED_COLUMNS = ["city", "country", "population", "temperature", "weather_description"]

def read_data(file_path):
    """
    Goal: Reads data from a specified file and loads it into a DataFrame.
    param data: Path to the file.
    return: DataFrame containing the data or None if an error occurs.
    """
    try:
        data = pd.read_csv(file_path)
        return data
    except Exception as e:
        print(f"Error reading data from {file_path}: {e}")
        raise AirflowException(f"Failed to read data from {file_path}: {e}")  # Raise AirflowException

def fetch_weather(city_name, api_key):
    """
    Goal: Fetches weather data for a specific city using the OpenWeatherMap API.
    param data: city_name: Name of the city and OpenWeatherMap API key.
    return: Dictionary containing weather data for the city or None if an error occurs.
    """
    try:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        weather_data = response.json()
        return {
            "city": weather_data.get("name"),
            "weather_description": weather_data["weather"][0]["description"] if weather_data.get("weather") else None,
            "temperature": weather_data["main"]["temp"] if weather_data.get("main") else None,
            "humidity": weather_data["main"]["humidity"] if weather_data.get("main") else None,
            "pressure": weather_data["main"]["pressure"] if weather_data.get("main") else None,
            "wind_speed": weather_data["wind"]["speed"] if weather_data.get("wind") else None,
            "clouds": weather_data["clouds"]["all"] if weather_data.get("clouds") else None,
        }
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data for {city_name}: {e}")
        raise AirflowException(f"Failed to fetch weather data for {city_name}: {e}")  # Raise AirflowException

def fetch_all_weather(cities, api_key):
    """
    Goal: Fetches weather data for all cities concurrently.
    param data: List of city names and OpenWeatherMap API key.
    return: List of dictionaries containing weather data for each city.
    """
    try:
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(lambda city: fetch_weather(city, api_key), cities))
        return [result for result in results if result is not None]
    except Exception as e:
        print(f"Error fetching weather data for multiple cities: {e}")
        raise AirflowException(f"Failed to fetch weather data for cities: {e}")  # Raise AirflowException

def load_to_database(db_path, table_name, data):
    """
    Goal: Loads the DataFrame into a SQLite database.
    param data: Path to the SQLite database, Name of the table to insert data into and DataFrame to be inserted.
    return: None
    """
    try:
        engine = create_engine(f'sqlite:///{db_path}')
        with engine.connect() as connection:
            data.to_sql(table_name, connection, if_exists='replace', index=False)
        print("Data loaded successfully into the database.")
    except Exception as e:
        print(f"Error loading data to database: {e}")
        raise AirflowException(f"Failed to load data into the database: {e}")  # Raise AirflowException

def clean_up(temp_files=None, engine=None):
    """
    Goal: Performs clean-up tasks such as closing connections and deleting temporary files.
    param data: List of temporary file paths to delete and SQLAlchemy engine to dispose of.
    return: None
    """
    if temp_files:
        for file in temp_files:
            try:
                os.remove(file)
                print(f"Deleted temporary file: {file}")
            except Exception as e:
                print(f"Error deleting file {file}: {e}")
    if engine:
        try:
            engine.dispose()
            print("Database connection closed.")
        except Exception as e:
            print(f"Error during clean-up: {e}")

if __name__ == "__main__":
    try:
        # File details
        file_path = "/opt/airflow/dags/cities.csv"

        # Load city data
        city_data = read_data(file_path)

        if city_data is not None and "city" in city_data.columns:
            # Fetch weather data concurrently
            weather_data_list = fetch_all_weather(city_data["city"], API_KEY)

            if weather_data_list:
                # Convert weather data to DataFrame
                weather_df = pd.DataFrame(weather_data_list)

                # Merge city data with weather data
                merged_data = pd.merge(city_data, weather_df, on="city", how="inner")

                # Select relevant columns
                final_data = merged_data[SELECTED_COLUMNS]

                # Print final data for verification
                print(final_data)

                # Load data into the database
                load_to_database(DB_PATH, TABLE_NAME, final_data)

                #clean up

                clean_up(None, None)

                print("All tasks completed successfully.")




    except AirflowException as e:
        print(f"Task failed due to: {e}")
        raise  # Re-raise the exception to ensure the task is marked as failed
