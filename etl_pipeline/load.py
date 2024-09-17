import psycopg2
import logging
import os
import pandas as pd
import re


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
            password=os.getenv('DB_PASSWORD'),
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
            cur.execute("""--sql
            CREATE TABLE IF NOT EXISTS dim_disaster_groups (
                id VARCHAR PRIMARY KEY,
                name VARCHAR,
                parent_id VARCHAR REFERENCES dim_disaster_groups(id)
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
                id VARCHAR PRIMARY KEY,
                name VARCHAR,
                parent_id VARCHAR REFERENCES dim_disaster_types(id)
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
                name_id VARCHAR PRIMARY KEY,
                event_name VARCHAR
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
                location_id VARCHAR PRIMARY KEY,
                country VARCHAR,
                ISO VARCHAR,
                region VARCHAR,
                continent VARCHAR,
                location VARCHAR
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
                date_id VARCHAR PRIMARY KEY,
                date DATE
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
                id VARCHAR PRIMARY KEY,
                name VARCHAR,
                parent_id VARCHAR REFERENCES dim_associated_distructions(id)
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
                OFDA_resp_id VARCHAR PRIMARY KEY,
                ofda_response VARCHAR
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
                appeal_id VARCHAR PRIMARY KEY,
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
                declaration_id VARCHAR PRIMARY KEY,
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
                dis_mag_scale_id VARCHAR PRIMARY KEY,
                dis_mag_scale VARCHAR
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
                adm_level_id VARCHAR PRIMARY KEY,
                adm_level VARCHAR
            );
            """)
            logging.info("Table dim_adm_levels created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_adm_levels: {e}")

        # dim_disasters_origin
        try:
            logging.info("Creating table: dim_disasters_origin")
            cur.execute("""--sql
            CREATE TABLE IF NOT EXISTS dim_disasters_origin (
                origin_id VARCHAR PRIMARY KEY,
                origin VARCHAR
            );
            """)
            logging.info("Table dim_disasters_origin created successfully.")
        except Exception as e:
            logging.error(f"Error creating dim_disasters_origin: {e}")

        # fact_disasters
        try:
            logging.info("Creating table: fact_disasters")
            cur.execute(""" --sql
                CREATE TABLE IF NOT EXISTS fact_disasters (
                id VARCHAR PRIMARY KEY ,
                seq BIGINT,
                glide VARCHAR,
                aid_contribution BIGINT,
                dis_mag_value BIGINT,
                total_deaths BIGINT,
                no_injured BIGINT,
                no_affected BIGINT,
                no_homeless BIGINT,
                total_affected BIGINT,
                insured_damages DOUBLE PRECISION,
                total_damages DOUBLE PRECISION,
                cpi DOUBLE PRECISION,
                extraction_time DATE,
                duration_days BIGINT,
                type_id VARCHAR REFERENCES dim_disaster_types(id),
                group_id VARCHAR REFERENCES dim_disaster_groups(id),
                associated_dis_id VARCHAR REFERENCES dim_associated_distructions(id),
                location_id VARCHAR REFERENCES dim_locations(location_id),
                name_id VARCHAR REFERENCES dim_disaster_names(name_id),
                OFDA_resp_id VARCHAR REFERENCES dim_ofda_responses(OFDA_resp_id),
                appeal_id VARCHAR REFERENCES dim_appeals(appeal_id),
                declaration_id VARCHAR REFERENCES dim_declarations(declaration_id),
                dis_mag_scale_id VARCHAR REFERENCES dim_mag_scales(dis_mag_scale_id),
                adm_level_id VARCHAR REFERENCES dim_adm_levels(adm_level_id),
                origin_id VARCHAR REFERENCES dim_disasters_origin(origin_id),
                starting_date_id VARCHAR REFERENCES dim_dates(date_id),
                ending_date_id VARCHAR REFERENCES dim_dates(date_id)
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
    # Convert date columns to datetime, handling invalid parsing as NaT
    df[start_date_col] = pd.to_datetime(df[start_date_col], errors='coerce')
    df[end_date_col] = pd.to_datetime(df[end_date_col], errors='coerce')
    
    # Determine the min and max dates
    min_date = min(df[start_date_col].min(), df[end_date_col].min())
    max_date = max(df[start_date_col].max(), df[end_date_col].max())

    # Create a DataFrame with all dates in the range
    all_dates = pd.date_range(start=min_date, end=max_date, freq='D').to_frame(name='date')

    # Create the date_dimension DataFrame with IDs
    date_dimension = all_dates.reset_index(drop=True)
    date_dimension[id_name] = range(1, len(date_dimension) + 1)
    date_dimension[id_name] = date_dimension[id_name].astype(int)
    
    # Merge date IDs into the original DataFrame for start_date_col
    df = pd.merge(df, date_dimension, left_on=start_date_col, right_on='date', how='left')
    df = df.rename(columns={id_name: f'{start_date_col}_id'}).drop(columns='date')
    
    # Merge date IDs into the original DataFrame for end_date_col
    df = pd.merge(df, date_dimension, left_on=end_date_col, right_on='date', how='left')
    df = df.rename(columns={id_name: f'{end_date_col}_id'}).drop(columns='date')
    
    # Drop rows where IDs are NaN
    df = df.dropna(subset=[f'{start_date_col}_id', f'{end_date_col}_id'])
    
    # Ensure IDs are integers
    df[f'{start_date_col}_id'] = df[f'{start_date_col}_id'].astype(int)
    df[f'{end_date_col}_id'] = df[f'{end_date_col}_id'].astype(int)
    
    return df, date_dimension

# Function to generate incremental ID for specified columns and new dimensions
##############################################################################
def create_incremental_ids(df, column_names, id_column_name):
    """
    This function generates incremental IDs for unique combinations of values across multiple columns in a DataFrame
    and ensures the IDs appear as the first column in the unique_combinations DataFrame.
    """
    unique_combinations = df[column_names].drop_duplicates().reset_index(drop=True)
    unique_combinations[id_column_name] = range(1, len(unique_combinations) + 1)
    
    unique_combinations = unique_combinations[[id_column_name] + [col for col in unique_combinations.columns if col != id_column_name]]
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
    df=add_id_column(df,'id')
    dim_disaster_types, df = create_hierarchy(df, ['disaster_type', 'disaster_subtype', 'disaster_subsubtype'],'type_id')
    dim_disaster_groups ,df= create_hierarchy(df,['disaster_group', 'disaster_subgroup'],'group_id')
    dim_associated_distructions, df=create_hierarchy(df,['associated_dis', 'associated_dis2'],'associated_dis_id')
    
    df, dim_locations=create_incremental_ids(df,['country', 'iso', 'region', 'continent', 'location'],'location_id')
    df, dim_disaster_names=create_incremental_ids(df,['event_name'],'name_id')
    df, dim_ofda_responses=create_incremental_ids(df,['ofda_response'],'ofda_resp_id')
    df, dim_appeals=create_incremental_ids(df,['appeal'],'appeal_id')
    df, dim_declarations=create_incremental_ids(df,['declaration'],'declaration_id')
    df, dim_mag_scales=create_incremental_ids(df,['dis_mag_scale'],'dis_mag_scale_id')
    df, dim_adm_levels=create_incremental_ids(df,['adm_level'],'adm_level_id')
    df, dim_disasters_origin=create_incremental_ids(df,['origin'],'origin_id')
    df, dim_dates=generate_date_ids(df,'starting_date','ending_date','date_id')
    return df, dim_disaster_types, dim_disaster_groups, dim_associated_distructions, dim_locations, dim_disaster_names, dim_ofda_responses, dim_appeals, dim_declarations, dim_mag_scales, dim_adm_levels,dim_disasters_origin,dim_dates


def load_dataframe_to_db(df, table_name, conn):
    """
    Load a DataFrame into a specified table in the database using psycopg2, 
    handling special characters in column names by sanitizing them.
    """
    if df.empty:
        logging.warning(f"No data to load for table: {table_name}")
        return

    try:
        # Create a cursor object
        cursor = conn.cursor()

        # Sanitize column names: replace invalid characters with underscores
        df.columns = [re.sub(r"[^\w]", "_", col) for col in df.columns]

        # Generate SQL query
        columns = ', '.join([f'"{col}"' for col in df.columns])  # Add double quotes around column names
        values = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

        # Convert DataFrame rows to list of tuples
        data = [tuple(row) for row in df.to_numpy()]

        # Execute batch insert
        cursor.executemany(insert_query, data)
        
        # Commit changes
        conn.commit()
        logging.info(f"Data successfully loaded into table: {table_name}")
        
    except Exception as e:
        logging.error(f"Error loading data into table {table_name}: {e}")
        conn.rollback()  # Rollback in case of error
        raise
    finally:
        cursor.close()  # Close cursor

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
