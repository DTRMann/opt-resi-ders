# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 22:11:40 2025

@author: DTRManning
"""

#TODO pull temperature and irradiance for load data. Make sure approach can
# accommodate additional weather data in the future as well.

import pandas as pd
import requests
from datetime import datetime, timedelta

def fetch_historical_weather_data(
    latitude: float, 
    longitude: float, 
    start_date: str, 
    end_date: str, 
    variables=None,
    timezone: str = 'Etc/UTC'
) -> pd.DataFrame:
    """
    Fetch historical weather data with configurable variables
    
    Parameters:
    -----------
    latitude : float
        Location latitude
    longitude : float
        Location longitude
    start_date : str
        Start date in 'YYYY-MM-DD' format
    end_date : str
        End date in 'YYYY-MM-DD' format
    variables : list or None
        List of weather variables to fetch. If None, defaults to temperature and
        shortwave irradiance data necessary for load calculation.
    timezone : str
        Timezone identifier (Region/City)
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing hourly historical weather data
    """
    
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    
    # Default variables for load calculation
    default_variables = [
        "temperature_2m",            # Temperature at 2m (°C)
        "shortwave_radiation"        # Total solar radiation on horizontal surface (W/m²)
    ]
    
    # Use provided variables or default to ones needed for load calculation
    hourly_variables = variables if variables is not None else default_variables
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": hourly_variables,
        "timezone": timezone
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Initialize DataFrame with timestamp
        df = pd.DataFrame({'timestamp': pd.to_datetime(data['hourly']['time'])})
        
        # Add all requested variables to DataFrame
        for var in hourly_variables:
            if var in data['hourly']:
                df[var] = data['hourly'][var]
            else:
                print(f"Warning: Variable '{var}' not available in API response")
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Example coordinates (Denver)
    lat = 39.7392
    lon = -104.9903
    tz  = 'America/Denver'
    
    # Get historical data for the previous year
    end_date = datetime.now() - timedelta(days=1) # End date is yesterday to avoid nans    
    start_date = (end_date - timedelta(days=365)).strftime('%Y-%m-%d')
    end_date   = end_date.strftime('%Y-%m-%d')

    df = fetch_historical_weather_data(lat, lon, start_date, end_date, None, tz)

