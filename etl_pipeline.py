import pandas as pd
import requests
import json
from pymongo import MongoClient
from datetime import datetime

import schedule
import time

from pymongo import DESCENDING
import pytz


# # Function to load the configuration from the JSON file
def load_config(file_path):
    with open(file_path, "r") as file:
        config = json.load(file)
    return config


# # Path to your db_config.json file
config_file_path = "config/db_config.json"

# # Load the config
config = load_config(config_file_path)
print(config)

# # Accessing the API key from the config
client = MongoClient(config.get("mongo_url"))
mongo_db = client[config.get("db")]
collection = mongo_db[config.get("collection")]

# --- Extract Functions ---


def extract_csv(path):
    return pd.read_csv(path)


def extract_json(path):
    with open(path, "r") as f:
        data = json.load(f)
    return pd.json_normalize(data)


def extract_google_sheet(path):
    return pd.read_csv(path)


def extract_openweathermap(city="Lahore", api_key="your_api_key_here"):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return pd.json_normalize(data)
    else:
        print(f"API error: {response.status_code}")
        return pd.DataFrame()


# --- Transform Function ---


def clean_weather_data(
    df,
    source_name,
    temp_col,
    temp_unit="C",
    humidity_col=None,
    wind_col=None,
    location_col=None,
    timestamp_col=None,
):
    df = df.drop_duplicates()
    df = df.fillna("N/A")

    if temp_unit == "F":
        df["temperature_c"] = (df[temp_col].astype(float) - 32) * 5.0 / 9.0
    else:
        df["temperature_c"] = df[temp_col].astype(float)

    df["humidity"] = (
        df[humidity_col].astype(float) if humidity_col and humidity_col in df else None
    )
    df["wind_speed"] = (
        df[wind_col].astype(float) if wind_col and wind_col in df else None
    )
    df["location"] = (
        df[location_col] if location_col and location_col in df else "Unknown"
    )

    if timestamp_col and timestamp_col in df:
        print("a")
        df["timestamp"] = pd.to_datetime(
            df[timestamp_col], errors="coerce"
        ).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        df["timestamp"] = pd.to_datetime(datetime.utcnow()).isoformat()

    try:
        df["weather_score"] = (
            df["temperature_c"].astype(float) * 0.4
            + df["humidity"].astype(float) * 0.3
            + df["wind_speed"].astype(float) * 0.3
        ).round(2)
    except:
        df["weather_score"] = None

    df["source"] = source_name

    return df[
        [
            "source",
            "timestamp",
            "location",
            "temperature_c",
            "humidity",
            "wind_speed",
            "weather_score",
        ]
    ]


# --- Load Function ---


def load_to_mongo(df):
    # Convert df timestamp to UTC datetime objects
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # Get the latest timestamp in MongoDB (UTC-aware)
    latest_entry = collection.find_one(
        sort=[("timestamp", DESCENDING)], projection={"timestamp": 1}
    )

    if latest_entry and latest_entry.get("timestamp"):
        latest_timestamp = pd.to_datetime(latest_entry["timestamp"], utc=True)

        # Filter rows strictly greater than the latest timestamp
        new_df = df[df["timestamp"] > latest_timestamp]
        print(f"🕓 Latest timestamp in DB: {latest_timestamp}")
    else:
        new_df = df
        print("ℹ No existing records found. Will insert all.")

    # Insert new rows if any
    if not new_df.empty:
        collection.insert_many(new_df.to_dict(orient="records"))
        print(f"✅ Inserted {len(new_df)} new records.")
    else:
        print("⚠ No new data to insert.")


# --- ETL Runner ---


def run_etl():
    print("🌦 Starting Weather ETL Pipeline...")

    # Replace with your real OpenWeatherMap API key
    OPENWEATHER_API_KEY = "your_api_key_here"

    # Extract
    csv_data = extract_csv("data/sample_data.csv")
    json_data = extract_json("data/sample_weather.json")
    sheet_data = extract_google_sheet("data/google_sheet_sample.csv")
    api_data = extract_openweathermap(city="Karachi", api_key=OPENWEATHER_API_KEY)

    # Transform
    weather_dfs = [
        clean_weather_data(
            csv_data,
            "CSV",
            temp_col="temp_f",
            temp_unit="F",
            humidity_col="humidity",
            wind_col="wind",
            location_col="city",
        ),
        clean_weather_data(
            json_data,
            "JSON",
            temp_col="main.temp",
            temp_unit="C",
            humidity_col="main.humidity",
            wind_col="wind.speed",
            location_col="name",
        ),
        clean_weather_data(
            sheet_data,
            "GoogleSheet",
            temp_col="temperature_c",
            temp_unit="C",
            humidity_col="humidity",
            wind_col="wind_kph",
            location_col="location_name",
        ),
        # clean_weather_data(api_data, "OpenWeatherMap", temp_col="main.temp", temp_unit='C', humidity_col="main.humidity", wind_col="wind.speed", location_col="name")
    ]

    # Merge
    merged_df = pd.concat(weather_dfs, ignore_index=True)

    # Load
    load_to_mongo(merged_df)

    print("✅ Weather ETL Pipeline Completed Successfully!")


if _name_ == "_main_":
    run_etl()