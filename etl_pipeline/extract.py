import psycopg2
from psycopg2 import sql
import io
import csv
import logging
import os


__all__ = [
    'connect_db',
    'get_columns_from_csv',
    'create_staging_table',
    'load_data_into_staging',
    'process_csv_to_staging',
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
#     filename=os.path.join(logs_dir, 'extract.log'),
#     level=logger.DEBUG,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )


# Connecting to PostgreSQL database
###################################
def connect_db():
    """
    Connects to the PostgreSQL database.
    Returns a connection object if successful, or None if connection fails.
    """
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'staging_disasters'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'Michel2003'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', 5432)
        )
        logger.info("Connected to database successfully")
        return conn
    except Exception as error:
        logger.error(f"Error connecting to the database: {error}")
        return None


# Extracting column names from CSV file
#######################################
def get_columns_from_csv(file_path):
    """
    Extracts column names from the CSV file.
    Returns a list of column names.
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
            reader = csv.reader(file)
            columns = next(reader)
        logger.info(f"Extracted columns from CSV file {file_path} successfully")
        return columns
    except Exception as error:
        logger.error(f"Error reading CSV file {file_path}: {error}")
        return []


# Creating the staging tables
#############################
def create_staging_table(cursor, table_name, columns):
    """
    Creates a staging table in the database with the given table name and columns.
    """
    try:
        columns_sql = ",\n".join(f'"{col}" TEXT' for col in columns)
        columns_sql += ",\n extraction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        
        create_table_query = sql.SQL("""--sql
            CREATE TABLE IF NOT EXISTS {} (
                {}
            );
        """).format(
            sql.Identifier(table_name),
            sql.SQL(columns_sql)
        )
        
        cursor.execute(create_table_query)
        logger.info(f"Created staging table {table_name} successfully")
    except Exception as error:
        logger.error(f"Error creating table {table_name}: {error}")


# Loading the raw data into staging area
########################################
def load_data_into_staging(cursor, table_name, file_path, columns):
    """
    Loads data from a CSV file into the staging table.
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
            file_content = file.read()
        
        file_like_object = io.StringIO(file_content)
        columns_list = ', '.join([f'"{col}"' for col in columns])

        copy_query = sql.SQL("""--sql
            COPY {} ({}) FROM STDIN WITH (FORMAT CSV, HEADER, NULL '', QUOTE '"');
        """).format(
            sql.Identifier(table_name),
            sql.SQL(columns_list)
        )
        
        cursor.copy_expert(sql=copy_query, file=file_like_object)
        logger.info(f"Data loaded into staging table {table_name} successfully")
    except Exception as error:
        logger.error(f"Error loading data into staging table {table_name}: {error}")


# Main Function to process CSV files
####################################
def process_csv_to_staging(cursor, table_name, file_path):
    """
    Processes a CSV file and loads its data into the staging table.
    """
    columns = get_columns_from_csv(file_path)
    if columns:
        create_staging_table(cursor, table_name, columns)
        load_data_into_staging(cursor, table_name, file_path, columns)
    else:
        logger.warning(f"No columns found for file {file_path}. Skipping processing.")


# Main execution flow
#####################
if __name__ == "__main__":
    conn = connect_db()
    if conn:
        cursor = conn.cursor()

        process_csv_to_staging(cursor, "staging_disasters", r"C:\Users\Legion\Desktop\official_data\1900_2021_DISASTERS.csv")

        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Database connection closed successfully")
    else:
        logger.error("Failed to connect to the database.")

# have environment variables for the database connection.