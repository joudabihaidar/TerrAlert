import logging
from extract import *
from transform import *
from load import * 

from datetime import datetime

# Set up logging
logging.basicConfig(filename='etl_logs.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

if __name__ == "__main__":

    try:
        ###########
        # Extract #
        ###########

        logger.info("Starting extraction process...")

        # Connect to database
        conn = connect_db('staging_disasters')
        if conn:
            cursor = conn.cursor()

            # Process CSV file and load data into staging table
            process_csv_to_staging(cursor, "staging_disasters", r"C:\Users\Legion\Desktop\official_data\1900_2021_DISASTERS.csv")

            conn.commit()
            cursor.close()
            conn.close()

            logger.info("Extraction completed successfully.")
        else:
            logger.error("Database connection failed.")
        
        #############
        # Transform #
        #############

        logger.info("Starting transformation process...")

        disasters = get_data_from_db(f""" --sql
                SELECT * FROM staging_disasters 
                WHERE TO_CHAR(extraction_time, 'YYYYMMDD') = '{datetime.now().strftime("%Y%m%d")}';
            """, 'staging_disasters')

        # Define columns to remove
        columns_to_remove = ['Local Time', 'River Basin', 'Admin1 Code', 'Admin2 Code', 'Geo Locations']

        # Apply transformations
        if disasters is not None:
            transformed_disasters = transform_data(disasters, columns_to_remove)
            logger.info("Transformation completed successfully.")
        else:
            logger.warning("No data found for transformation.")

    except Exception as e:
        logger.error(f"Error occurred: {e}")

