# Google_Cloud
This repository contains Google Cloud Functions to automate the collection of flight, weather, population, and airport data. It integrates data from external APIs and web scraping into a MySQL database on Google Cloud SQL, with scheduled updates via Google Cloud Scheduler.

## Key Features:
### Flight Data Integration: 
Fetches and stores flight-related information from an external API, including flight number, airline, arrival, departure information, and flight times.
### Weather Data Collection: 
Automatically collects and stores weather information for specific cities, including current temperature, humidity, and weather conditions.
### Population Data Retrieval:
Scrapes Wikipedia for population data of cities and updates the database regularly.
### Airport Data Integration: 
Fetches detailed airport data, including airport name, ICAO code, location (latitude, longitude), and associated city, and stores it in the database.
### Scheduled Updates: 
Uses Google Cloud Scheduler to trigger these functions at scheduled intervals, ensuring the data is updated regularly (e.g., yearly for population data, daily for weather or flight updates).
### MySQL Database Integration: 
All the data is integrated into a MySQL database (hosted on Google Cloud SQL), with proper management to avoid duplicates and ensure data consistency.
