import os
import pandas as pd

def extract_subarea(place):
    return place[0]


def extract_area(place):
    return place[-1]


def extract_date(time):
    return str(time).split(' ')[0]


def extract_weekday(time):
    date = extract_date(time)
    return date + ' - ' + str(time.weekday())


def extract_hour(time):
    t = str(time).split(' ')
    return t[0] + ' - ' + t[1].split(':')[0]



def fetch_eq_data(period='daily', region='Worldwide', min_mag=1):
    # Where we are getting data from
    url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{}.csv'

    if period == 'weekly':
        new_url = url.format('all_week')
    elif period == 'monthly':
        new_url = url.format('all_month')
    else:
        new_url = url.format('all_day')


    df_earthquake = pd.read_csv(new_url)
    
    # Extracting sub-area in place cols
    place_list = df_earthquake['place'].str.split(', ')    
    df_earthquake['sub_area'] = place_list.apply(extract_subarea)
    df_earthquake['area'] = place_list.apply(extract_area)
    df_earthquake = df_earthquake.drop(columns=['place'], axis=1)

    # Filtering data based on min. mag threshold
    if isinstance(min_mag, int) and min_mag > 0:
        df_earthquake = df_earthquake[df_earthquake['mag'] >= min_mag]
    else:
        df_earthquake = df_earthquake[df_earthquake['mag'] > 0]

    # Converting 'time' to pd datetime
    df_earthquake['time'] = pd.to_datetime(df_earthquake['time'])
    
    # Setting lat and long to some default if not found
    if region in df_earthquake['area'].to_list():
        df_earthquake = df_earthquake[df_earthquake['area'] == region]
        max_mag = df_earthquake['mag'].max()
        center_lat = df_earthquake[df_earthquake['mag'] == max_mag]['latitude'].values[0]
        center_long = df_earthquake[df_earthquake['mag'] == max_mag]['longitude'].values[0]
    else:
        center_lat, center_long = [54,15]

    df_earthquake = df_earthquake.sort_values(by='time')

    return df_earthquake



def store_eq_data(df, folder='API_data', filename='earthquake_data.csv'):
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


df_earthquake=fetch_eq_data('monthly','Worlwide',1)
store_eq_data(df_earthquake)