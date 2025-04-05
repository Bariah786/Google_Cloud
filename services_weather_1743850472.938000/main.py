import functions_framework
import dotenv
import os
dotenv.load_dotenv()
import pandas as pd
import requests
import datetime

@functions_framework.http
def weather_info(request):
    
    # Initialize lists for storing data across multiple cities
    all_city_data_parameters = []

    temperature= []
    min_temp= []
    max_temp= []
    humidity= []
    wind_speed= []
    description= []
    date= []
    time_retrieved= []

    #API_key for weather api
    api_key= os.environ['Weather_API']

    schema = "ganz"
    host = "34.38.92.221"
    user = os.environ['user_name']
    password = os.environ['MYSQL_password']
    port = 3306

    connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    cities= pd.read_sql('SELECT City_id, City, Latitude, Longitude FROM cities', con= connection_string)
      

    for index, row in cities.iterrows():
        Latitude = row['Latitude']
        Longitude = row['Longitude']
        City= row['City']
        
        weather_api= requests.get(f'https://api.openweathermap.org/data/2.5/forecast?lat={Latitude}&lon={Longitude}&appid={api_key}&units=metric')

        #Create json
        weather_json =  weather_api.json()

        for x in range(len(weather_json['list'])):
            #Temperature 
            temperature.append(weather_json['list'][x]['main']['temp'])

            #Min_Temperature & max_Temperature
            min_temp.append(weather_json['list'][x]['main']['temp_min'])
            max_temp.append(weather_json['list'][x]['main']['temp_max'])

            #Humidity 
            humidity.append(weather_json['list'][x]['main']['humidity'])

            #Wind Speed
            wind_speed.append(weather_json['list'][x]['wind']['speed'])

            #Description
            description.append(weather_json['list'][x]['weather'][0]['description'])

            #Time of record
            date.append(weather_json['list'][x]['dt_txt'])

            #Time when data retrieved
            time_retrieved.append(datetime.datetime.now().strftime('%Y/%m/%d, %H:%M'))

        #Creating DF for weather info
        Parameters= pd.DataFrame({'City': City,
                        'Date':date,
                        'Temperature': temperature,
                        'Minimum_Temperature': min_temp,
                        'Maximum_Temperature': max_temp,
                        'Humidity': humidity,
                        'Wind_Speed': wind_speed,
                        'Description': description,
                        'Time_Retrieved': time_retrieved}
      )
        #Append the current city's data to the all_city_data list
        all_city_data_parameters .append(Parameters)

    #Concatenate all the dataframes in the list to a single dataframe
    all_city_data_parameters = pd.concat(all_city_data_parameters, ignore_index=True)

    #Creating weather_df
    weather_df = all_city_data_parameters

    #Make sure Date and time_retrived is in datetime format
    weather_df['Date']= pd.to_datetime(weather_df['Date'])
    weather_df['Time_Retrieved']= pd.to_datetime(weather_df['Time_Retrieved'])

    # Merging two dataframes (cities & weather_df) to get weather_info with city_id rather than city and dropping columns we don't require
    city_weather_info= pd.merge(weather_df, cities, how='left', on='City')
    city_weather_info= city_weather_info.drop(['City', 'Latitude', 'Longitude'], axis= 1)

    # Pushing info to sql database
    #Transfer it to sql
    city_weather_info.to_sql('weather',
                        if_exists = 'append',
                        con= connection_string,
                        index= False)
                        
    return 'Weather data update completed successfully.'


