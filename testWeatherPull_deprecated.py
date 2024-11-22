# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 11:19:14 2024

@author: DTRManning
"""

import os

rootDir = 'C:/Users/DTRManning/Desktop/OptimizeResiGenSizing'

os.chdir(rootDir)

from getWeatherData import get_historical_weather_data, calculate_ghi, calculate_gti,\
                            calculate_solar_production, fetch_solar_radiation_data

### Test historical weather data pull ###
# Example usage:
latitude = 37.7749
longitude = -122.4194
variables = ["temperature_2m"]

# Fetch hourly historical weather data
weather_data = get_historical_weather_data(
    latitude = latitude,
    longitude = longitude,
    variables = variables,
    start_date = "2023-11-01",
    end_date = "2023-11-07",
    tz = "America/Denver")

print(weather_data)

# Example location and parameters
latitude = 37.7749
longitude = -122.4194
tilt = 30  # Tilt angle in degrees
azimuth = 180  # Azimuth angle in degrees (South)
panel_area = 1 # Area of one panel in m²
panel_efficiency = 0.2  # 20% efficiency
start_date = "2023-11-01"
end_date = "2023-11-07"
tz = "America/Denver"

# Fetch data
solar_data = fetch_solar_radiation_data(latitude, longitude, start_date, end_date, tz)

# Extract hourly data
direct_radiation = solar_data["hourly"]["direct_radiation"]
diffuse_radiation = solar_data["hourly"]["diffuse_radiation"]

# Initialize lists for results
gti_list = []
production_list = []

# Calculate GHI, GTI, and production for each hour
for direct, diffuse in zip(direct_radiation, diffuse_radiation):
    ghi = calculate_ghi(direct, diffuse)
    gti = calculate_gti(ghi, tilt, azimuth)
    production = calculate_solar_production(gti, panel_area, panel_efficiency)

    gti_list.append(gti)
    production_list.append(production)

# Add results to the data
solar_data["hourly"]["gti"] = gti_list
solar_data["hourly"]["solar_production_kW"] = production_list

# Output results
print(solar_data)