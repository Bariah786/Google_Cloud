# Google Cloud

This repository contains Google Cloud Functions to automate the collection of flight, weather, population, and airport data. It integrates data from external APIs and web scraping into a MySQL database hosted on Google Cloud SQL, with scheduled updates via Google Cloud Scheduler.

## Key Features:

- **Flight Data Integration:** Fetches and stores flight-related information, including flight number, airline, arrival/departure information, and flight times.
  
- **Weather Data Collection:** Collects and stores real-time weather data for cities, including temperature, humidity, and weather conditions.
  
- **Population Data Retrieval:** Scrapes Wikipedia for city population data and updates the database regularly.
  
- **Airport Data Integration:** Retrieves detailed airport data, including airport name, ICAO code, location, and associated city, and stores it in the database.
  
- **Scheduled Updates:** Uses Google Cloud Scheduler to trigger the functions at scheduled intervals (e.g., yearly for population data, daily for weather and flight updates).

- **MySQL Database Integration:** Data is stored and managed in a MySQL database on Google Cloud SQL, with mechanisms in place to avoid duplicates and ensure consistency.

## Technologies Used:

- **Google Cloud Functions:** Serverless compute service for executing the functions.
  
- **Google Cloud Scheduler:** Automates function execution at specified intervals.
  
- **MySQL (Google Cloud SQL):** Relational database for storing and managing data.

- **External APIs:** Used for flight, weather, and airport data collection.
  
- **Web Scraping:** Scrapes Wikipedia for city population data.
  
- **Python:** The primary language for writing functions.

- **Libraries:**
  - `requests` for making API calls.
  - `BeautifulSoup` for web scraping.
  - `pandas` for data manipulation and SQL integration.
  - `sqlalchemy` and `pymysql` for SQL database interaction. etc

## Core Functions:

- **Population Data Fetching:** Scrapes population data for cities from Wikipedia and stores it in the database.
  
- **Airport Data Fetching:** Retrieves airport data (ICAO code, airport name, location) via an external API and stores it.
  
- **Flight Data Integration:** Collects and stores real-time flight data (flight number, airline, arrival/departure info) from an API.
  
- **Weather Data Integration:** Gathers and stores real-time weather information for cities.
  
- **Scheduled Execution:** Functions are triggered at scheduled intervals using Google Cloud Scheduler to keep the database updated.

## Setup Instructions:

1. **Create a Google Cloud Project:** If you haven't already, create a Google Cloud project.
2. **Deploy Google Cloud Functions:** Use the `gcloud` CLI or Google Cloud Console to deploy the functions.
3. **API Keys Setup:** Set up API Keys for external services (flight, weather, airport APIs) and store them as environment variables in Google Cloud.
4. **Configure Cloud Scheduler:** Set up Cloud Scheduler to trigger functions at the desired intervals (e.g., annually for population data, daily for weather/flight data).
5. **Set Up Google Cloud SQL:** Create a MySQL instance on Google Cloud SQL to host your database.
6. **Set Permissions:** Ensure Google Cloud Functions have proper permissions to interact with Google Cloud SQL.

