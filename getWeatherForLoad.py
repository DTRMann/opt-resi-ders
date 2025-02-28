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
import matplotlib.pyplot as plt # For validation plots

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

def validate_weather_data_distributions(weather_df):
    """
    Create distribution plots for each weather variable to validate data reasonableness.
    
    Parameters:
    -----------
    weather_df : pandas.DataFrame
        DataFrame containing weather data
    """
    if weather_df is None or weather_df.empty:
        print("ERROR: No data retrieved.")
        return
    
    # Skip timestamp column
    numeric_cols = [col for col in weather_df.columns if col != 'timestamp']
    
    if not numeric_cols:
        print("No numeric columns to plot")
        return
    
    # Create subplots - one per variable
    fig, axes = plt.subplots(len(numeric_cols), 1, figsize=(10, 3*len(numeric_cols)))
    
    # If only one column, axes won't be an array
    if len(numeric_cols) == 1:
        axes = [axes]
    
    for i, col in enumerate(numeric_cols):
        # Create histogram
        axes[i].hist(weather_df[col], bins=50, alpha=0.7, color='skyblue')
        
        # Add title and labels
        axes[i].set_title(f'Distribution of {col}')
        axes[i].set_xlabel(col)
        axes[i].set_ylabel('Frequency')
        
        # Add vertical line for mean
        mean = weather_df[col].mean()
        axes[i].axvline(mean, color='r', linestyle='--', label=f'Mean: {mean:.2f}')
        
        # Print some basic statistics
        min_val = weather_df[col].min()
        max_val = weather_df[col].max()
        print(f"{col}: min={min_val:.2f}, max={max_val:.2f}, mean={mean:.2f}")
        
        axes[i].legend()
    
    plt.tight_layout()
    plt.show()



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

    # Run the simplified validation
    validate_weather_data_distributions(df) 
    
