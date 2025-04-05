import functions_framework
import dotenv
import os 
dotenv.load_dotenv()
import requests
from bs4 import BeautifulSoup
from lat_lon_parser import parse
import pandas as pd
import datetime
from datetime import datetime, timedelta
from pytz import timezone
import time
import re

@functions_framework.http
#Creating Function for Population of city/cities

def population_and_flights(request):
  city_pop_data = []  # List to store the city info
  schema = "ganz"
  host = "34.38.92.221"
  user = os.environ['user_name']
  password = os.environ['MYSQL_password']
  port = 3306

  connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
  #Calling cities from SQL
  cities_from_sql= pd.read_sql('SELECT * FROM cities', con= connection_string)

  # Iterating over each city name in the DataFrame
  for index, row in cities_from_sql.iterrows():  
     city_name = row['City'] 
     url = f'https://en.wikipedia.org/wiki/{city_name}' # Using city_name for the url
     response = requests.get(url)
     if response.status_code != 200:
            print(f"Failed to load page for {city_name}. Status code: {response.status_code}")
            continue
     else:
            city_soup = BeautifulSoup(response.content, 'html.parser')  # assign to city_soup


            pop = city_soup.find(string='Population').find_next(string=re.compile(r'\d+,\d+,\d+'))
            pop = pop.replace(',', '')  #in case want to remove commas
            pop = int(pop)
            times= datetime.now().strftime("%Y-%m-%d %H:%M:%S")



# Appending city info to the list
     city_pop_data.append({'City': city_name, 'Population': pop, 'Timestamp_data_retrieved': times})

  population_df= pd.DataFrame(city_pop_data)

#Merge 2 dataframes
  merged_df= population_df.merge(cities_from_sql,
                              on= 'City',
                              how= 'left')

  merged_df= merged_df.drop(columns= ['Country', 'Latitude', 'Longitude'])

  #Transfer it to sql

  merged_df.to_sql('population',
                 if_exists= 'append',
                 con= connection_string,
                 index= False)  

  #Airports df
  # Fetching latitude and longitude of the cities from SQL
  lat = cities_from_sql['Latitude'].to_list()
  lon = cities_from_sql['Longitude'].to_list()

  # API URL and headers
  url = "https://aerodatabox.p.rapidapi.com/airports/search/location"
  headers = {
        "x-rapidapi-key": os.environ['Airports_Flights_API'],
        "x-rapidapi-host": "aerodatabox.p.rapidapi.com"
    }

  # Initialize an empty DataFrame to store airport data
  city_airport_df = pd.DataFrame()
  airports_list = []

  # Loop through each city (latitude, longitude) to fetch airport data
  for lat, lon in zip(lat, lon):
        querystring = {"lat": lat, "lon": lon, "radiusKm": "50", "limit": "10", "withFlightInfoOnly": "true"}

        # Add a fixed 3-second delay before each request to avoid hitting API rate limits
        time.sleep(3)  # Sleep for 3 seconds before sending the request

        # Send the API request
        response = requests.get(url, headers=headers, params=querystring)

        # If the request is successful, process the data
        if response.status_code == 200:
            airport_json = response.json()  # Getting the JSON data from the response
            airport_df = pd.json_normalize(airport_json.get('items', []))
            airport_df.rename(columns={'municipalityName': 'City', 'name': 'Airport_Name', 'shortName': 'Short_Name',
                                    'icao': 'ICAO', 'countryCode': 'Country_Code', 'location.lat': 'Location_lat',
                                    'location.lon': 'Location_lon'}, inplace=True)
            # Append the data to the list
            airports_list.append(airport_df)

  # Concatenate data of all cities
  city_airport_df = pd.concat(airports_list, ignore_index=True)
  airports_df = city_airport_df

  # Drop unnecessary columns
  airports_df.drop(columns=['iata', 'timeZone'], inplace=True)

  # Merge with the cities DataFrame to get city_id
  airports_new_df = airports_df.merge(cities_from_sql, how='left', on='City')
  
  airports_new_df.drop(columns=['Latitude', 'Longitude', 'Country'], inplace=True)
  
  #Checking existed ICAOS
  existing_icaos_df = pd.read_sql("SELECT ICAO FROM airports", con=connection_string)
  existing_icaos = existing_icaos_df['ICAO'].tolist()

  # Filter the new data to include only ICAOs that don't already exist in the table
  new_airports_df = airports_new_df[~airports_new_df['ICAO'].isin(existing_icaos)]

  # Insert only new ICAOs into the airports table
  if not new_airports_df.empty:  #if new_airports_df is not empty
    new_airports_df.to_sql('airports', con=connection_string, if_exists='append', index=False)

  return 'Population and Airports data is uploaded successfully.'
