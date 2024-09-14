import psycopg2
import logging
import os
import pandas as pd

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the path to the logs folder
logs_dir = os.path.join(script_dir, 'logs')

# Ensure the logs directory exists
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Configure logging to store logs in the logs folder
logging.basicConfig(
    filename=os.path.join(logs_dir, 'load.log'),
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
            dbname=os.getenv('DB_NAME', 'disasters_dwh'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'Michel2003'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', 5432)
        )
        logging.info("Connected to database successfully")
        return conn
    except Exception as error:
        logging.error(f"Error connecting to the database: {error}")
        return None


def create_disaster_tables(conn):
    try:
        cur = conn.cursor()

        # dim_disaster_groups
        try:
            logging.info("Creating table: dim_disaster_groups")
            cur.execute("""--sql
            CREATE TABLE IF NOT EXISTS dim_disaster_groups (
                group_id INT PRIMARY KEY,
                group_name VARCHAR,
                parent_group_id INT REFERENCES dim_disaster_groups(group_id)
            );
            """)
            logging.info("Table dim_disaster_groups created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_disaster_groups: {e}")

        # dim_disaster_types
        try:
            logging.info("Creating table: dim_disaster_types")
            cur.execute("""--sql
            CREATE TABLE IF NOT EXISTS dim_disaster_types (
                type_id INT PRIMARY KEY,
                type_name VARCHAR,
                parent_type_id INT REFERENCES dim_disaster_types(type_id)
            );
            """)
            logging.info("Table dim_disaster_types created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_disaster_types: {e}")

        # dim_disaster_names
        try:
            logging.info("Creating table: dim_disaster_names")
            cur.execute("""--sql
            CREATE TABLE IF NOT EXISTS dim_disaster_names (
                name_id INT PRIMARY KEY,
                name VARCHAR
            );
            """)
            logging.info("Table dim_disaster_names created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_disaster_names: {e}")

        # dim_locations
        try:
            logging.info("Creating table: dim_locations")
            cur.execute("""--sql
            CREATE TABLE IF NOT EXISTS dim_locations (
                location_id INT PRIMARY KEY,
                longitude DOUBLE PRECISION,
                latitude DOUBLE PRECISION,
                country VARCHAR,
                country_code VARCHAR,
                city VARCHAR,
                state VARCHAR
            );
            """)
            logging.info("Table dim_locations created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_locations: {e}")

        # dim_dates
        try:
            logging.info("Creating table: dim_dates")
            cur.execute("""--sql
            CREATE TABLE IF NOT EXISTS dim_dates (
                date_id INT PRIMARY KEY,
                disaster_date DATE
            );
            """)
            logging.info("Table dim_dates created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_dates: {e}")

        # dim_associated_distructions
        try:
            logging.info("Creating table: dim_associated_distructions")
            cur.execute("""--sql
            CREATE TABLE IF NOT EXISTS dim_associated_distructions (
                associated_dis_id INT PRIMARY KEY,
                associated_dis VARCHAR,
                parent_id INT REFERENCES dim_associated_distructions(associated_dis_id)
            );
            """)
            logging.info("Table dim_associated_distructions created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_associated_distructions: {e}")

        # dim_ofda_responses
        try:
            logging.info("Creating table: dim_ofda_responses")
            cur.execute("""--sql
            CREATE TABLE IF NOT EXISTS dim_ofda_responses (
                OFDA_resp_id INT PRIMARY KEY,
                OFDA_resp VARCHAR
            );
            """)
            logging.info("Table dim_ofda_responses created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_ofda_responses: {e}")

        # dim_appeals
        try:
            logging.info("Creating table: dim_appeals")
            cur.execute("""--sql
            CREATE TABLE IF NOT EXISTS dim_appeals (
                appeal_id INT PRIMARY KEY,
                appeal VARCHAR
            );
            """)
            logging.info("Table dim_appeals created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_appeals: {e}")

        # dim_declarations
        try:
            logging.info("Creating table: dim_declarations")
            cur.execute("""--sql
            CREATE TABLE IF NOT EXISTS dim_declarations (
                declaration_id INT PRIMARY KEY,
                declaration VARCHAR
            );
            """)
            logging.info("Table dim_declarations created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_declarations: {e}")

        # dim_mag_scales
        try:
            logging.info("Creating table: dim_mag_scales")
            cur.execute("""--sql
            CREATE TABLE IF NOT EXISTS dim_mag_scales (
                dis_mag_scale_id INT PRIMARY KEY,
                dis_mag_scalle VARCHAR
            );
            """)
            logging.info("Table dim_mag_scales created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_mag_scales: {e}")

        # dim_adm_levels
        try:
            logging.info("Creating table: dim_adm_levels")
            cur.execute("""--sql
            CREATE TABLE IF NOT EXISTS dim_adm_levels (
                adm_level_id INT PRIMARY KEY,
                adm_level INT
            );
            """)
            logging.info("Table dim_adm_levels created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_adm_levels: {e}")

        # fact_disasters
        try:
            logging.info("Creating table: fact_disasters")
            cur.execute("""--sql
            CREATE TABLE IF NOT EXISTS fact_disasters (
                disaster_id INT PRIMARY KEY,
                seq INT,
                glide VARCHAR,
                starting_date_id INT REFERENCES dim_dates(date_id),
                ending_date_id INT REFERENCES dim_dates(date_id),
                group_id INT REFERENCES dim_disaster_groups(group_id),
                type_id INT REFERENCES dim_disaster_types(type_id),
                name_id INT REFERENCES dim_disaster_names(name_id),
                location_id INT REFERENCES dim_locations(location_id),
                duration INT,
                origin VARCHAR,
                associated_dis_id INT REFERENCES dim_associated_distructions(associated_dis_id),
                OFDA_resp_id INT REFERENCES dim_ofda_responses(OFDA_resp_id),
                appeal_id INT REFERENCES dim_appeals(appeal_id),
                declaration_id INT REFERENCES dim_declarations(declaration_id),
                aid_contribution INT,
                dis_mag_value INT,
                dis_mag_scale_id INT REFERENCES dim_mag_scales(dis_mag_scale_id),
                total_deaths INT,
                no_injured INT,
                no_affected INT,
                no_homeless INT,
                total_affected INT,
                insured_damages DOUBLE PRECISION,
                total_damages DOUBLE PRECISION,
                cpi DOUBLE PRECISION,
                adm_level_id INT REFERENCES dim_adm_levels(adm_level_id)
            );
            """)
            logging.info("Table fact_disasters created successfully.")
        except Exception as e:
            logging.error(f"Error creating fact_disasters: {e}")

        conn.commit()
        logging.info("All tables created successfully.")

    except Exception as e:
        logging.error(f"General error during table creation: {e}")
        conn.rollback()

    finally:
        cur.close()

if __name__=="__main__":
    create_disaster_tables(connect_db())