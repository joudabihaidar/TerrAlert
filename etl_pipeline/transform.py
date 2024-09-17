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
from datetime import datetime


__all__ = [
    'connect_db',
    'get_data_from_db',
    'clean_column_names',
    'combine_date',
    'compute_dates',
    'duration_days',
    'add_reverse_geocode_info',
    'try_parsing_date',
    'remove_duplicates',
    'remove_columns',
    'transform_data',
    'rename_column'
]

logger = logging.getLogger(__name__)

# # directory where the script is located
# script_dir = os.getcwd()
# # logs folder
# logs_dir = os.path.join(script_dir, 'logs')
# if not os.path.exists(logs_dir):
#     os.makedirs(logs_dir)
# # configuring logger to store logs in the logs folder
# logger.basicConfig(
#     filename=os.path.join(logs_dir, 'transform.log'),
#     level=logger.DEBUG,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )


# Connecting to PostgreSQL database
###################################
def connect_db(dbname):
    """
    Connects to the PostgreSQL database.
    Returns a connection object if successful, or None if connection fails.
    """
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user='postgres',
            password='Michel2003',
            host='localhost',
            port=5432
        )
        logger.info("Connected to database successfully")
        return conn
    except Exception as error:
        logger.error(f"Error connecting to the database: {error}")
        return None


# Function to get data from PostgreSQL and load into a pandas DataFrame
#######################################################################
def get_data_from_db(query,dbname):
    """
    Fetches data from the PostgreSQL database using the provided query.
    Cleans column names and returns the data as a pandas DataFrame.
    """
    conn = connect_db(dbname)
    if conn is None:
        logger.error("Connection to database failed")
        return None
    
    try:
        df = pd.read_sql_query(query, conn)
        logger.info(f"Data fetched successfully for query: {query}")
        return df
    except Exception as error:
        logger.error(f"Error fetching data: {error}")
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
    logger.info("Column names cleaned")
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
        logger.warning(f"Invalid date encountered: Year={year}, Month={month}, Day={day}")
        return pd.NaT


# Function to compute the start and end dates in a DataFrame
############################################################
def compute_dates(df: pd.DataFrame, start_cols: list, end_cols: list, dataset_name: str) -> pd.DataFrame:
    """
    Computes the start and end dates based on year, month, and day columns in the DataFrame.
    Adds 'start_date' and 'end_date' columns to the DataFrame.
    """
    df['starting_date'] = df.apply(lambda row: combine_date(row[start_cols[0]], row[start_cols[1]], row[start_cols[2]]), axis=1)
    df['ending_date'] = df.apply(lambda row: combine_date(row[end_cols[0]], row[end_cols[1]], row[end_cols[2]]), axis=1)
    logger.info(f"Start and end dates computed for {dataset_name} dataset")
    return df


def rename_column(df, old_name, new_name):
    """
    Renames a column in a pandas DataFrame.
    """
    df = df.rename(columns={old_name: new_name})
    return df


# Function to compute the duration in days between start_date and end_date
##########################################################################
def duration_days(df: pd.DataFrame, start_column: str, end_column: str, dataset_name: str) -> pd.DataFrame:
    """
    Computes the duration in days between start and end columns.
    Adds a 'duration_days' column to the DataFrame.
    """
    df['duration_days'] = (df[end_column] - df[start_column]).dt.days
    logger.info(f"Duration in days computed between {start_column} and {end_column} for {dataset_name} dataset")
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
        logger.info(f"Reverse geocoding information added for {dataset_name} dataset using columns {lat_col} and {lon_col}")
    except Exception as error:
        logger.error(f"Error during reverse geocoding for {dataset_name} dataset: {error}")
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
        logger.warning(f"Date parsing failed for text: {text}")
        return pd.NaT
    

# Function to remove duplicates from DataFrame
################################################
def remove_duplicates(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Removes duplicate rows from the DataFrame.
    """
    original_count = len(df)
    df = df.drop_duplicates()
    removed_count = original_count - len(df)
    logger.info(f"Removed {removed_count} duplicates from {dataset_name} dataset")
    return df


# Function to remove specified columns from DataFrame
#####################################################
def remove_columns(df: pd.DataFrame, columns: list, dataset_name: str) -> pd.DataFrame:
    """
    Removes the specified columns from the DataFrame.
    """
    df = df.drop(columns=columns, errors='ignore')
    logger.info(f"Removed columns {columns} from {dataset_name} dataset")
    return df


# Transform function that applies cleaning, transformations, geocoding, duplicates, and column removal
#######################################################################################################
def transform_data(disasters: pd.DataFrame, columns_to_remove: list = None): 
    """
    Transforms disasters  data by applying various transformations.
    Removes duplicates and specific columns if provided.
    Returns the transformed DataFrames.
    """
    try:

        # Remove duplicates
        disasters = remove_duplicates(disasters, 'disasters')

        # Process disasters DataFrame
        disasters = clean_column_names(disasters)
        disasters = compute_dates(disasters, ['start_year', 'start_month', 'start_day'], ['end_year', 'end_month', 'end_day'], 'disasters')
        disasters = duration_days(disasters, 'starting_date', 'ending_date', 'disasters')
        
        # Remove specified columns
        if columns_to_remove:
            disasters = remove_columns(disasters, columns_to_remove, 'disasters')

        logger.info("Disasters data transformation completed")
        return disasters
    except Exception as error:
        logger.error(f"Error during data transformation: {error}")
        return disasters


# if __name__ == "__main__":
    # # Fetching data
    # current_date = datetime.now().strftime("%Y%m%d")
    # disasters = get_data_from_db( f""" --sql
    # SELECT * FROM staging_disasters 
    # WHERE TO_CHAR(extraction_time, 'YYYYMMDD') = '{current_date}';
    # """,'disasters_staging')
    # columns_to_remove=['Local Time','River Basin','Admin1 Code','Admin2 Code','Geo Locations']    

    # # Applying transformations
    # if disasters is not None: 
    #     transformed_disasters= transform_data(disasters,columns_to_remove)
    # else:
    #     logger.error("Data fetch failed. Transformations not applied.")

    
    