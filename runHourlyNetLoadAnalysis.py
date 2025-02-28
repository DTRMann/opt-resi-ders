# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 14:45:02 2025

@author: DTRManning
"""



from datetime import datetime, time
import os

os.chdir('C:\\Users\\DTRManning\\Desktop\\OptimizeResiGenSizing')

from getGTIData import fetch_historical_gti_data
from getPOAData import calculate_poa
from getWeatherForLoad import fetch_historical_weather_data
from estimateResidentialLoad import HybridLoadModel

# Query parameters
lat = 39.7392
lon = -104.9903
tz  = 'America/Denver'
tilt = 30
azimuth = 180

start_date = datetime.strptime('2024-01-01', "%Y-%m-%d").date()
end_date = datetime.strptime('2024-12-31', "%Y-%m-%d").date()

# Get historical irradiance data
df = fetch_historical_gti_data(lat, lon, start_date, end_date, 
                             panel_tilt=30, panel_azimuth=180,
                             timezone = tz)

# Get POA
df1 = calculate_poa(df)

# Get weather for load data
df_weatherLoad = fetch_historical_weather_data(lat, lon, start_date, end_date, None, tz)

# Simulate load
model = HybridLoadModel()
predictions = model.predict_load(
    df_weatherLoad,
    house_size=2000
)


