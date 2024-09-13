# read about environement variables
MAP_KEY='cd6439c1c84a672bdcd7fbbc5c44d30a' 

import os
import pandas as pd

# (is it good practice to import the libraries needed inside a function?)

# def get_transaction_count(MAP_KEY):
#     url = 'https://firms.modaps.eosdis.nasa.gov/mapserver/mapkey_status/?MAP_KEY=' + MAP_KEY
#     count=0
#     try:
#         df=pd.read_json(url, typ='series')
#         count=df['current_transactions']
#     except:
#         print("Error in our call")
#     return count

# def fetch_new_fire_data(MAP_KEY):
#     area_url = 'https://firms.modaps.eosdis.nasa.gov/api/area/csv/' + MAP_KEY + '/VIIRS_NOAA20_NRT/world/1'  
#     start_count=get_transaction_count(MAP_KEY)
#     df_wildfires=pd.read_csv(area_url)
#     end_count=get_transaction_count(MAP_KEY)
#     tcount=get_transaction_count(MAP_KEY)
#     print ('Our current transaction count is %i' % tcount)
#     print ('We used %i transactions.' % (end_count-start_count))      

    
def fetch_new_fire_data(MAP_KEY):
    area_url = 'https://firms.modaps.eosdis.nasa.gov/api/area/csv/' + MAP_KEY + '/VIIRS_NOAA20_NRT/world/1'  
    df_wildfires=pd.read_csv(area_url)
    return df_wildfires


def store_wildfire_data(df, folder='API_data', filename='wildfire_data.csv'):
    if not os.path.exists(folder):
        os.makedirs(folder)

    filepath = os.path.join(folder, filename)
    if os.path.exists(filepath):
        existing_df = pd.read_csv(filepath)
        combined_df = pd.concat([existing_df, df])
        combined_df = combined_df.drop_duplicates()
    else:
        combined_df = df
    combined_df.to_csv(filepath, index=False)
    print(f"Data stored in {filepath}")




df_wildfire=fetch_new_fire_data(MAP_KEY)
store_wildfire_data(df_wildfire)


# Things you still have to do:
# Environment variables
# label the wildfire data into their country code name
# automate the whole process using airflow.
# get the population density
# get the resources

# if i could get both of these correctly, the project would be perfect
