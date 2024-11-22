# -*- coding: utf-8 -*-
"""
Created on Fri Nov 15 14:19:20 2024

@author: DTRManning
"""
import pandas as pd
import numpy as np
import requests
import math

### Get weather data ###
def getHistoricalWeatherData(latitude, longitude, variables, start_date, end_date, tz):
    """
    Fetches historical hourly weather data from the Open Meteo API.

    Parameters:
    - latitude (float): Latitude of the location.
    - longitude (float): Longitude of the location.
    - variables (list): List of weather variables to fetch (e.g., ["temperature_2m", "precipitation"]).
    - start_date (str): Start date for data in YYYY-MM-DD format.
    - end_date (str): End date for data in YYYY-MM-DD format.
    - tz (str): Timezone

    Returns:
    - dict: Parsed JSON response with weather data or an error message.
    """
    base_url = "https://archive-api.open-meteo.com/v1/era5"  # Endpoint for historical data
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(variables),
        "timezone": tz
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        return {"error": str(e)}

# Example usage:
latitude = 37.7749
longitude = -122.4194
variables = ["temperature_2m"]

# Fetch hourly historical weather data
weather_data = getHistoricalWeatherWata(
    latitude = latitude,
    longitude = longitude,
    variables = variables,
    start_date = "2023-11-01",
    end_date = "2023-11-07",
    tz = "America/Denver")

print(weather_data)


def getHistoricalIrradData(latitude, longitude, variables, tilt, azimuth, start_date, end_date, tz):
    """
    Fetches historical hourly weather data, including Global Tilted Irradiance (GHI),
    from the Open Meteo API with specific tilt and azimuth for solar radiation calculation.

    Parameters:
    - latitude (float): Latitude of the location.
    - longitude (float): Longitude of the location.
    - variables (list): List of weather variables to fetch (e.g., ["temperature_2m", "precipitation"]).
    - tilt (float): Tilt angle of the surface for irradiance calculation (degrees).
    - azimuth (float): Azimuth angle of the surface for irradiance calculation (degrees).
    - start_date (str): Start date for data in YYYY-MM-DD format.
    - end_date (str): End date for data in YYYY-MM-DD format.
    - tz (str): Timezone

    Returns:
    - dict: Parsed JSON response with weather data or an error message.
    """
    base_url = "https://archive-api.open-meteo.com/v1/era5"  # Endpoint for historical data
    
    # Include solar radiation in the requested variables
    #if "solar_radiation" not in variables:
    #    variables.append("solar_radiation")

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(variables),
        "timezone": tz,
        "tilt": tilt,  # Pass tilt for GHI calculation
        "azimuth": azimuth  # Pass azimuth for GHI calculation
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        return {"error": str(e)}

# Example usage
latitude = 40.7128  # New York City latitude
longitude = -74.0060  # New York City longitude
variables = ["global_tilted_irradiance"]  # Additional variables as needed
tilt = 30  # 30-degree tilt
azimuth = 0  # South-facing azimuth
start_date = "2024-11-15"
end_date = "2024-11-15"
tz = "America/Denver"  # Timezone

data = getHistoricalIrradData(latitude, longitude, variables, tilt, azimuth, start_date, end_date, tz)

# Output the data (or process it further)
print(data)

# Run the main function
if __name__ == "__main__":
    results = main()
    print(results)








