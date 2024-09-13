import pandas as pd
import reverse_geocode
import json
import numpy as np
import csv 
import os
from dateutil import parser
import psycopg2
from psycopg2 import sql
import logging

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the path to the logs folder
logs_dir = os.path.join(script_dir, 'logs')

# Ensure the logs directory exists
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Configure logging to store logs in the logs folder
logging.basicConfig(
    filename=os.path.join(logs_dir, 'transform.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Connecting to PostgreSQL database
###################################
def connect_db():
    """
    Connects to the PostgreSQL database.
    Returns a connection object if successful, or None if connection fails.
    """
    try:
        conn = psycopg2.connect(
            dbname='staging_disasters',
            user='postgres',
            password='Michel2003',
            host='localhost',
            port=5432
        )
        logging.info("Connected to database successfully")
        return conn
    except Exception as error:
        logging.error(f"Error connecting to the database: {error}")
        return None


# Function to get data from PostgreSQL and load into a pandas DataFrame
#######################################################################
def get_data_from_db(query):
    """
    Fetches data from the PostgreSQL database using the provided query.
    Cleans column names and returns the data as a pandas DataFrame.
    """
    conn = connect_db()
    if conn is None:
        logging.error("Connection to database failed")
        return None
    
    try:
        df = pd.read_sql_query(query, conn)
        df = clean_column_names(df)
        logging.info(f"Data fetched successfully for query: {query}")
        return df
    except Exception as error:
        logging.error(f"Error fetching data: {error}")
        return None
    finally:
        conn.close()


# Function to clean column names (removes spaces, lowercases)
#############################################################
def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans DataFrame column names by converting to lowercase and replacing spaces with underscores.
    """
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    logging.info("Column names cleaned")
    return df


# Function to combine year, month, and day into a date
######################################################
def combine_date(year: int, month: int, day: int) -> pd.Timestamp:
    """
    Combines year, month, and day into a datetime object.
    Returns NaT if the date is invalid.
    """
    try:
        date_str = f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        return pd.to_datetime(date_str, errors='coerce')
    except (ValueError, TypeError):
        logging.warning(f"Invalid date encountered: Year={year}, Month={month}, Day={day}")
        return pd.NaT


# Function to compute the start and end dates in a DataFrame
############################################################
def compute_dates(df: pd.DataFrame, start_cols: list, end_cols: list, dataset_name: str) -> pd.DataFrame:
    """
    Computes the start and end dates based on year, month, and day columns in the DataFrame.
    Adds 'start_date' and 'end_date' columns to the DataFrame.
    """
    df['start_date'] = df.apply(lambda row: combine_date(row[start_cols[0]], row[start_cols[1]], row[start_cols[2]]), axis=1)
    df['end_date'] = df.apply(lambda row: combine_date(row[end_cols[0]], row[end_cols[1]], row[end_cols[2]]), axis=1)
    logging.info(f"Start and end dates computed for {dataset_name} dataset")
    return df


# Function to compute the duration in days between start_date and end_date
##########################################################################
def duration_days(df: pd.DataFrame, start_column: str, end_column: str, dataset_name: str) -> pd.DataFrame:
    """
    Computes the duration in days between start and end columns.
    Adds a 'duration_days' column to the DataFrame.
    """
    df['duration_days'] = (df[end_column] - df[start_column]).dt.days
    logging.info(f"Duration in days computed between {start_column} and {end_column} for {dataset_name} dataset")
    return df


# Function to add reverse geocoding information to a DataFrame
##############################################################
def add_reverse_geocode_info(df: pd.DataFrame, lat_col: str = 'dfo_centroid_y', lon_col: str = 'dfo_centroid_x', dataset_name: str = 'dataset') -> pd.DataFrame:
    """
    Adds reverse geocoding information (country, country_code, city, state) to the DataFrame based on latitude and longitude.
    """
    try:
        coords = list(zip(df[lat_col], df[lon_col]))
        results = reverse_geocode.search(coords)
        geocode_df = pd.DataFrame(results)[['country', 'country_code', 'city', 'state']]
        df[['country', 'country_code', 'city', 'state']] = geocode_df
        logging.info(f"Reverse geocoding information added for {dataset_name} dataset using columns {lat_col} and {lon_col}")
    except Exception as error:
        logging.error(f"Error during reverse geocoding for {dataset_name} dataset: {error}")
    return df


# Function to parse dates safely
################################
def try_parsing_date(text: str) -> pd.Timestamp:
    """
    Tries to parse a date from a string. Returns NaT if parsing fails.
    """
    try:
        return parser.parse(text)
    except (ValueError, TypeError):
        logging.warning(f"Date parsing failed for text: {text}")
        return pd.NaT


# Transform function that applies cleaning, transformations, and geocoding
##########################################################################
def transform_data(disasters: pd.DataFrame, floods: pd.DataFrame, storms: pd.DataFrame) -> tuple:
    """
    Transforms disasters, floods, and storms data by applying various transformations.
    Returns the transformed DataFrames.
    """
    try:
        # Process disasters DataFrame
        disasters = compute_dates(disasters, ['start_year', 'start_month', 'start_day'], ['end_year', 'end_month', 'end_day'], 'disasters')
        disasters = duration_days(disasters, 'start_date', 'end_date', 'disasters')
        logging.info("Disasters data transformation completed")

        # Process floods DataFrame
        floods = add_reverse_geocode_info(floods, dataset_name='floods')
        floods['dfo_began'] = floods['dfo_began'].apply(try_parsing_date)
        floods['dfo_ended'] = floods['dfo_ended'].apply(try_parsing_date)
        floods = duration_days(floods, 'dfo_began', 'dfo_ended', 'floods')
        logging.info("Floods data transformation completed")

        # Process storms DataFrame
        storms = clean_column_names(storms)
        storms = add_reverse_geocode_info(storms, 'lat', 'long', 'storms')
        storms = compute_dates(storms, ['year', 'month', 'day'], ['year', 'month', 'day'], 'storms')
        logging.info("Storms data transformation completed")

        return disasters, floods, storms
    except Exception as error:
        logging.error(f"Error during data transformation: {error}")
        return disasters, floods, storms

if __name__ == "__main__":
    # Fetch data
    disasters = get_data_from_db("SELECT * FROM staging_disasters;")
    floods = get_data_from_db("SELECT * FROM staging_floods;")
    storms = get_data_from_db("SELECT * FROM staging_storms;")

    # Apply transformations
    if disasters is not None and floods is not None and storms is not None:
        transformed_disasters, transformed_floods, transformed_storms = transform_data(disasters, floods, storms)
    else:
        logging.error("Data fetch failed. Transformations not applied.")

    
    