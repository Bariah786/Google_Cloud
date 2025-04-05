import functions_framework
import pandas as pd
import requests
from datetime import datetime, timedelta
from pytz import timezone
import time
import dotenv
import os
dotenv.load_dotenv()

@functions_framework.http

def tomorrows_flight_arrivals(request):

    api_key = os.environ['Airports_Flights_API']
    berlin_timezone = timezone('Europe/Berlin')
    today = datetime.now(berlin_timezone).date()
    tomorrow = today + timedelta(days=1)

    list_for_arrivals_df = []

    schema = "ganz"
    host = "34.38.92.221"
    user = os.environ['user_name']
    password = os.environ['MYSQL_password']
    port = 3306

    connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    icaos = pd.read_sql('select ICAO from airports', con= connection_string)
    icao_list= icaos['ICAO'].to_list()

    for icao in icao_list:
        print(f"Processing ICAO: {icao}")

        times = [["00:00", "11:59"], ["12:00", "23:59"]]

        for time_period in times:
            url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{icao}/{tomorrow}T{time_period[0]}/{tomorrow}T{time_period[1]}"

            querystring = {"direction": "Arrival", "withCancelled": "false"}

            headers = {
                "X-RapidAPI-Key": api_key,
                "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
            }

            response = requests.get(url, headers=headers, params=querystring)

            if response.status_code == 429:
                print(f"Rate limit exceeded for ICAO {icao}. Waiting before retrying...")
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    time.sleep(int(retry_after))
                else:
                    time.sleep(60)  # Default to 60 seconds if 'Retry-After' is not provided
                response = requests.get(url, headers=headers, params=querystring)
            
            if response.status_code != 200:
                print(f"Error with response for ICAO {icao}: {response.status_code}")
                continue

            try:
                flights_resp = response.json()
            except ValueError as e:
                print(f"Failed to decode JSON for ICAO {icao}: {e}")
                continue

            if 'arrivals' in flights_resp and flights_resp['arrivals']:
                arrivals_df = pd.json_normalize(flights_resp["arrivals"])[
                    ["number", "airline.name", "movement.scheduledTime.local", "movement.terminal", "movement.airport.name", "movement.airport.icao"]]
                arrivals_df = arrivals_df.rename(columns={
                    "number": "Flight_number",
                    "airline.name": "Airline",
                    "movement.scheduledTime.local": "Local_arrival_time",
                    "movement.terminal": "Arrival_terminal",
                    "movement.airport.name": "Departure_city",
                    "movement.airport.icao": "Departure_airport_ICAO"
                })
                arrivals_df["Arrival_airport_ICAO"] = icao
                arrivals_df["Data_retrieved_on"] = datetime.now(berlin_timezone).strftime("%Y-%m-%d %H:%M:%S")

                arrivals_df["Local_arrival_time"] = arrivals_df["Local_arrival_time"].str.split("+").str[0]

                list_for_arrivals_df.append(arrivals_df)
            else:
                print(f"No arrivals found for ICAO {icao}")

        dataframe= pd.concat(list_for_arrivals_df, ignore_index=True)
    
    flights_df= dataframe

    # Pushing info to MYSQL
    flights_df.to_sql('flights',
                  if_exists= 'append',
                  index= False,
                  con= connection_string)
    return 'Flights data update completed successfully.'

