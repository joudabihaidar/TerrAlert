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

        columns_to_remove = ['Local Time', 'River Basin', 'Admin1 Code', 'Admin2 Code', 'Geo Locations']
        if disasters is not None:
            transformed_disasters = transform_data(disasters, columns_to_remove)
            logger.info("Transformation completed successfully.")
            print(transformed_disasters.columns)
        else:
            logger.warning("No data found for transformation.")

    except Exception as e:
        logger.error(f"Error occurred: {e}")

    ########
    # Load #
    ########
    logger.info("Starting load process...")

    try:
        ########
        # Load #
        ########
        logger.info("Starting load process...")

        # Connect to the data warehouse
        dwh_conn = connect_db('disasters_dwh')
        if dwh_conn:
            logger.info("Connected to the data warehouse successfully.")
            
            # Create disaster tables
            logger.info("Creating disaster tables...")
            create_disaster_tables(dwh_conn)
            logger.info("Disaster tables created successfully.")

            # Generate dimensions
            logger.info("Generating dimension tables...")
            fact_disasters, dim_disaster_types, dim_disaster_groups, dim_associated_distructions, dim_locations, dim_disaster_names, dim_ofda_responses, dim_appeals, dim_declarations, dim_mag_scales, dim_adm_levels, dim_disasters_origin, dim_dates = generate_dimensions(transformed_disasters)
            logger.info("Dimension tables generated successfully.")

            # Load fact_disasters
            logger.info("Loading fact_disasters...")
            load_fact_disasters(transformed_disasters, dwh_conn)
            logger.info("fact_disasters loaded successfully.")

            # Load dim_disaster_types
            logger.info("Loading dim_disaster_types...")
            load_dim_disaster_types(transformed_disasters, dwh_conn)
            logger.info("dim_disaster_types loaded successfully.")

            # Load dim_disaster_groups
            logger.info("Loading dim_disaster_groups...")
            load_dim_disaster_groups(transformed_disasters, dwh_conn)
            logger.info("dim_disaster_groups loaded successfully.")

            # Load dim_associated_distructions
            logger.info("Loading dim_associated_distructions...")
            load_dim_associated_distructions(transformed_disasters, dwh_conn)
            logger.info("dim_associated_distructions loaded successfully.")

            # Load dim_locations
            logger.info("Loading dim_locations...")
            load_dim_locations(transformed_disasters, dwh_conn)
            logger.info("dim_locations loaded successfully.")

            # Load dim_disaster_names
            logger.info("Loading dim_disaster_names...")
            load_dim_disaster_names(transformed_disasters, dwh_conn)
            logger.info("dim_disaster_names loaded successfully.")

            # Load dim_ofda_responses
            logger.info("Loading dim_ofda_responses...")
            load_dim_ofda_responses(transformed_disasters, dwh_conn)
            logger.info("dim_ofda_responses loaded successfully.")

            # Load dim_appeals
            logger.info("Loading dim_appeals...")
            load_dim_appeals(transformed_disasters, dwh_conn)
            logger.info("dim_appeals loaded successfully.")

            # Load dim_declarations
            logger.info("Loading dim_declarations...")
            load_dim_declarations(transformed_disasters, dwh_conn)
            logger.info("dim_declarations loaded successfully.")

            # Load dim_mag_scales
            logger.info("Loading dim_mag_scales...")
            load_dim_mag_scales(transformed_disasters, dwh_conn)
            logger.info("dim_mag_scales loaded successfully.")

            # Load dim_adm_levels
            logger.info("Loading dim_adm_levels...")
            load_dim_adm_levels(transformed_disasters, dwh_conn)
            logger.info("dim_adm_levels loaded successfully.")

            # Load dim_disasters_origin
            logger.info("Loading dim_disasters_origin...")
            load_dim_disasters_origin(transformed_disasters, dwh_conn)
            logger.info("dim_disasters_origin loaded successfully.")

            # Load dim_dates
            logger.info("Loading dim_dates...")
            load_dim_dates(transformed_disasters, dwh_conn)
            logger.info("dim_dates loaded successfully.")

            # Close the connection
            dwh_conn.close()
            logger.info("Data warehouse connection closed.")
        else:
            logger.error("Data warehouse connection failed.")

    except Exception as e:
        logger.error(f"Error occurred during loading process: {e}")