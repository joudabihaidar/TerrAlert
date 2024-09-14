import psycopg2
import logging
import os
import pandas as pd


logger = logging.getLogger(__name__)


# Connecting to PostgreSQL database
###################################
def connect_db(dbname):
    """
    Connects to the PostgreSQL database.
    Returns a connection object if successful, or None if connection fails.
    """
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME', dbname),
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
    if conn is None:
        logging.error("Connection is None. Cannot create tables.")
        return

    cur = None
    try:
        cur = conn.cursor()

        # dim_disaster_groups
        try:
            logging.info("Creating table: dim_disaster_groups")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_disaster_groups (
                id INT PRIMARY KEY,
                name VARCHAR,
                parent_id INT REFERENCES dim_disaster_groups(id)
            );
            """)
            logging.info("Table dim_disaster_groups created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_disaster_groups: {e}")

        # dim_disaster_types
        try:
            logging.info("Creating table: dim_disaster_types")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_disaster_types (
                id INT PRIMARY KEY,
                name VARCHAR,
                parent_id INT REFERENCES dim_disaster_types(id)
            );
            """)
            logging.info("Table dim_disaster_types created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_disaster_types: {e}")

        # dim_disaster_names
        try:
            logging.info("Creating table: dim_disaster_names")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_disaster_names (
                name_id INT PRIMARY KEY,
                event_name VARCHAR
            );
            """)
            logging.info("Table dim_disaster_names created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_disaster_names: {e}")

        # dim_locations
        try:
            logging.info("Creating table: dim_locations")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_locations (
                location_id INT PRIMARY KEY,
                country VARCHAR,
                ISO VARCHAR,
                region VARCHAR,
                continent VARCHAR,
                location VARCHAR,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION
            );
            """)
            logging.info("Table dim_locations created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_locations: {e}")

        # dim_dates
        try:
            logging.info("Creating table: dim_dates")
            cur.execute("""
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
            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_associated_distructions (
                associated_dis_id INT PRIMARY KEY,
                name VARCHAR,
                parent_id INT REFERENCES dim_associated_distructions(associated_dis_id)
            );
            """)
            logging.info("Table dim_associated_distructions created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_associated_distructions: {e}")

        # dim_ofda_responses
        try:
            logging.info("Creating table: dim_ofda_responses")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_ofda_responses (
                OFDA_resp_id INT PRIMARY KEY,
                ofda_response VARCHAR
            );
            """)
            logging.info("Table dim_ofda_responses created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_ofda_responses: {e}")

        # dim_appeals
        try:
            logging.info("Creating table: dim_appeals")
            cur.execute("""
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
            cur.execute("""
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
            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_mag_scales (
                dis_mag_scale_id INT PRIMARY KEY,
                dis_mag_scale VARCHAR
            );
            """)
            logging.info("Table dim_mag_scales created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_mag_scales: {e}")

        # dim_adm_levels
        try:
            logging.info("Creating table: dim_adm_levels")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_adm_levels (
                adm_level_id INT PRIMARY KEY,
                adm_level VARCHAR
            );
            """)
            logging.info("Table dim_adm_levels created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_adm_levels: {e}")

        # dim_disasters_origin
        try:
            logging.info("Creating table: dim_disasters_origin")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_disasters_origin (
                origin_id INT PRIMARY KEY,
                origin VARCHAR
            );
            """)
            logging.info("Table dim_disasters_origin created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_disasters_origin: {e}")

        # fact_disasters
        try:
            logging.info("Creating table: fact_disasters")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fact_disasters (
                id INT PRIMARY KEY,
                seq INT,
                glide VARCHAR,
                aid_contribution INT,
                dis_mag_value INT,
                starting_date_id INT REFERENCES dim_dates(date_id),
                ending_date_id INT REFERENCES dim_dates(date_id),
                total_deaths INT,
                no_injured INT,
                no_affected INT,
                no_homeless INT,
                total_affected INT,
                insured_damages DOUBLE PRECISION,
                total_damages DOUBLE PRECISION,
                cpi DOUBLE PRECISION,
                adm_level_id INT REFERENCES dim_adm_levels(adm_level_id),
                location_id INT REFERENCES dim_locations(location_id),
                group_id INT REFERENCES dim_disaster_groups(id),
                type_id INT REFERENCES dim_disaster_types(id),
                associated_dis_id INT REFERENCES dim_associated_distructions(associated_dis_id),
                name_id INT REFERENCES dim_disaster_names(name_id),
                OFDA_resp_id INT REFERENCES dim_ofda_responses(OFDA_resp_id),
                appeal_id INT REFERENCES dim_appeals(appeal_id),
                declaration_id INT REFERENCES dim_declarations(declaration_id),
                dis_mag_scale_id INT REFERENCES dim_mag_scales(dis_mag_scale_id),
                origin_id INT REFERENCES dim_disasters_origin(origin_id),
                duration INT
            );
            """)
            logging.info("Table fact_disasters created successfully.")
        except Exception as e:
            logging.error(f"Error creating fact_disasters: {e}")

        conn.commit()
        logging.info("All tables created successfully.")

    except Exception as e:
        logging.error(f"General error during table creation: {e}")
        if conn is not None:
            conn.rollback()

    finally:
        if cur is not None:
            cur.close()

# # Function to get data from PostgreSQL and load into a pandas DataFrame
# #######################################################################
# def get_data_from_db(query,conn):
#     """
#     Fetches data from the PostgreSQL database using the provided query.
#     Cleans column names and returns the data as a pandas DataFrame.
#     """
#     #conn = connect_db()
#     if conn is None:
#         logging.error("Connection to database failed")
#         return None
    
#     try:
#         df = pd.read_sql_query(query, conn)
#         logging.info(f"Data fetched successfully for query: {query}")
#         return df
#     except Exception as error:
#         logging.error(f"Error fetching data: {error}")
#         return None
#     finally:
#         conn.close()


# Function to create a hierarchy of groups and subgroups and Subsubgroups... 
############################################################################
def create_hierarchy(df, level_cols, id_col_name):
    """
    Create a hierarchy of groups and subgroups with incremental IDs, and add the the Ids to the original DataFrame.
    """
    hierarchy = pd.DataFrame(columns=['id', 'name', 'parent_id'])
    current_id = 1
    
    parent_ids = {}  # A dictionary to keep track of parent IDs for each group level

    df[id_col_name] = None
    
    for i, level in enumerate(level_cols):
        unique_values = df[level].unique()
        
        for value in unique_values:
            #  parent ID (if it's not the first level)
            if i > 0:
                # checking if there is a matching parent in the previous level
                parent_row = df[df[level] == value]
                if len(parent_row) > 0:
                    parent_value = parent_row[level_cols[i-1]].values[0]
                    parent_id = parent_ids.get(parent_value, None)
                else:
                    parent_id = None  # No matching parent found
            else:
                parent_id = None  # First level has no parent
     
            hierarchy = pd.concat([hierarchy, pd.DataFrame({
                'id': [current_id],
                'name': [value],
                'parent_id': [parent_id]
            })], ignore_index=True)
            
            parent_ids[value] = current_id
            df.loc[df[level] == value, id_col_name] = current_id
            
            current_id += 1
    df = df.drop(columns=level_cols)
    return hierarchy, df


# Function to generate all dates between the starting date and ending date
##########################################################################
def generate_date_ids(df, start_date_col, end_date_col, id_name='date_id'):
    """
    Generates incremental IDs for all dates between the minimum and maximum dates 
    found in the starting_date and ending_date columns, updates the original DataFrame 
    to replace dates with their corresponding IDs, and creates a new DataFrame with 
    all unique dates and their IDs.
    """

    df[start_date_col] = pd.to_datetime(df[start_date_col])
    df[end_date_col] = pd.to_datetime(df[end_date_col])
    
    min_date = min(df[start_date_col].min(), df[end_date_col].min())
    max_date = max(df[start_date_col].max(), df[end_date_col].max())

    all_dates = pd.date_range(start=min_date, end=max_date, freq='D').to_frame(name='Date')

    date_dimension = all_dates.reset_index(drop=True)
    date_dimension[id_name] = range(1, len(date_dimension) + 1)
    
    df = pd.merge(df, date_dimension, left_on=start_date_col, right_on='Date', how='left')
    df = df.rename(columns={id_name: f'{start_date_col}_id'}).drop(columns='Date')
    
    df = pd.merge(df, date_dimension, left_on=end_date_col, right_on='Date', how='left')
    df = df.rename(columns={id_name: f'{end_date_col}_id'}).drop(columns='Date')
    
    return df, date_dimension


# Function to generate incremental ID for specified columns and new dimensions
##############################################################################
def create_incremental_ids(df, column_names, id_column_name):
    """
    This function generates incremental IDs and can for unique combinations of values across multiple columns in a DataFrame.
    """
    unique_combinations = df[column_names].drop_duplicates().reset_index(drop=True)
    unique_combinations[id_column_name] = range(1, len(unique_combinations) + 1)
    df = pd.merge(df, unique_combinations, on=column_names, how='left')
    df = df.drop(columns=column_names)
    return df, unique_combinations


def add_id_column(df, id_column_name='id'):
    """
    Adds an auto-incrementing ID column to the fact table.
    """
    df[id_column_name] = range(1, len(df) + 1)

    df = df[[id_column_name] + [col for col in df.columns if col != id_column_name]]
    
    return df


# this function will take a dataframe and start spiltting it into the wanted dimensions
#######################################################################################
def generate_dimensions(df):
    dim_disaster_types, df = create_hierarchy(df, ['disaster_type', 'disaster_subtype', 'disaster_subsubtype'],'type_id')
    dim_disaster_groups ,df= create_hierarchy(df,['disaster_group', 'disaster_subgroup'],'group_id')
    dim_associated_distructions, df=create_hierarchy(df,['associated_dis', 'associated_dis2'],'associated_dis_id')
    
    df, dim_locations=create_incremental_ids(df,['country', 'iso', 'region', 'continent', 'location','latitude','longitude'],'location_id')
    df, dim_disaster_names=create_incremental_ids(df,['event_name'],'name_id')
    df, dim_ofda_responses=create_incremental_ids(df,['ofda_response'],'OFDA_resp_id')
    df, dim_appeals=create_incremental_ids(df,['appeal'],'appeal_id')
    df, dim_declarations=create_incremental_ids(df,['declaration'],'declaration_id')
    df, dim_mag_scales=create_incremental_ids(df,['dis_mag_scale'],'dis_mag_scale_id')
    df, dim_adm_levels=create_incremental_ids(df,['adm_level'],'adm_level_id')
    df, dim_disasters_origin=create_incremental_ids(df,['origin'],'origin_id')
    df, dim_dates=generate_date_ids(df,'start_date','end_date','date_id')
    return df, dim_disaster_types, dim_disaster_groups, dim_associated_distructions, dim_locations, dim_disaster_names, dim_ofda_responses, dim_appeals, dim_declarations, dim_mag_scales, dim_adm_levels,dim_disasters_origin,dim_dates


import pandas as pd
import psycopg2

def load_dataframe_to_db(df, table_name, conn):
    """
    Load a DataFrame into a specified table in the database.
    """
    if df.empty:
        print(f"No data to load for table: {table_name}")
        return

    try:
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Data loaded into table: {table_name}")
    except Exception as e:
        print(f"Error loading data into table {table_name}: {e}")
        raise

def load_fact_disasters(df, conn):
    load_dataframe_to_db(df, 'fact_disasters', conn)

def load_dim_disaster_types(df, conn):
    load_dataframe_to_db(df, 'dim_disaster_types', conn)

def load_dim_disaster_groups(df, conn):
    load_dataframe_to_db(df, 'dim_disaster_groups', conn)

def load_dim_associated_distructions(df, conn):
    load_dataframe_to_db(df, 'dim_associated_distructions', conn)

def load_dim_locations(df, conn):
    load_dataframe_to_db(df, 'dim_locations', conn)

def load_dim_disaster_names(df, conn):
    load_dataframe_to_db(df, 'dim_disaster_names', conn)

def load_dim_ofda_responses(df, conn):
    load_dataframe_to_db(df, 'dim_ofda_responses', conn)

def load_dim_appeals(df, conn):
    load_dataframe_to_db(df, 'dim_appeals', conn)

def load_dim_declarations(df, conn):
    load_dataframe_to_db(df, 'dim_declarations', conn)

def load_dim_mag_scales(df, conn):
    load_dataframe_to_db(df, 'dim_mag_scales', conn)

def load_dim_adm_levels(df, conn):
    load_dataframe_to_db(df, 'dim_adm_levels', conn)

def load_dim_disasters_origin(df, conn):
    load_dataframe_to_db(df, 'dim_disasters_origin', conn)

def load_dim_dates(df, conn):
    load_dataframe_to_db(df, 'dim_dates', conn)
