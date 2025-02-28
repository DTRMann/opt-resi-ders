# -*- coding: utf-8 -*-
"""
@author: DTRM
"""

import pandas as pd
import numpy as np
import requests

from datetime import datetime, timedelta


def calculate_solar_position(latitude: float, longitude: float, timestamp: pd.Timestamp, timezone: str) -> tuple:
    """
    Calculate solar position (elevation and azimuth) for a given time and location
    
    Returns:
    --------
    tuple (float, float)
        Solar elevation and azimuth angles in degrees
    """
    # Convert timestamp to day of year and hour of day
    day_of_year = timestamp.dayofyear
    hour = timestamp.hour + timestamp.minute/60
    
    # Calculate solar declination angle
    declination = 23.45 * np.sin(np.radians(360/365 * (day_of_year - 81)))
    
    # Calculate hour angle
    B = 360 * (day_of_year - 81) / 365
    E = 9.87 * np.sin(np.radians(2*B)) - 7.53 * np.cos(np.radians(B)) - 1.5 * np.sin(np.radians(B))
    solar_time = hour + 4 * (longitude - 15 * float(timezone)) / 60 + E/60
    hour_angle = 15 * (solar_time - 12)
    
    # Calculate solar elevation
    lat_rad = np.radians(latitude)
    dec_rad = np.radians(declination)
    hour_rad = np.radians(hour_angle)
    
    sin_elevation = (np.sin(lat_rad) * np.sin(dec_rad) + 
                    np.cos(lat_rad) * np.cos(dec_rad) * np.cos(hour_rad))
    elevation = np.degrees(np.arcsin(sin_elevation))
    
    # Calculate solar azimuth
    cos_azimuth = ((np.sin(dec_rad) * np.cos(lat_rad) - 
                    np.cos(dec_rad) * np.sin(lat_rad) * np.cos(hour_rad)) / 
                   np.cos(np.radians(elevation)))
    azimuth = np.degrees(np.arccos(np.clip(cos_azimuth, -1, 1)))
    
    if hour_angle > 0:
        azimuth = 360 - azimuth
        
    return elevation, azimuth

def calculate_gti(direct_radiation: float, diffuse_radiation: float, 
                 solar_elevation: float, solar_azimuth: float, 
                 panel_tilt: float, panel_azimuth: float, albedo: float = 0.2) -> float:
    """
    Calculate Global Tilted Irradiance (GTI) for a tilted surface
    
    Parameters:
    -----------
    direct_radiation : float
        Direct normal irradiance (W/m²)
    diffuse_radiation : float
        Diffuse horizontal irradiance (W/m²)
    solar_elevation : float
        Solar elevation angle in degrees
    solar_azimuth : float
        Solar azimuth angle in degrees
    panel_tilt : float
        Panel tilt angle from horizontal in degrees (0 = horizontal, 90 = vertical)
    panel_azimuth : float
        Panel azimuth angle in degrees (0 = North, 90 = East, 180 = South, 270 = West)
    albedo : float
        Ground reflectance coefficient (default = 0.2 for typical ground)
        
    Returns:
    --------
    float
        Global tilted irradiance (W/m²)
    """
    # Convert angles to radians
    solar_elevation_rad = np.radians(solar_elevation)
    solar_azimuth_rad = np.radians(solar_azimuth)
    panel_tilt_rad = np.radians(panel_tilt)
    panel_azimuth_rad = np.radians(panel_azimuth)
    
    # Calculate incidence angle
    cos_incidence = (np.sin(solar_elevation_rad) * np.cos(panel_tilt_rad) +
                    np.cos(solar_elevation_rad) * np.sin(panel_tilt_rad) *
                    np.cos(solar_azimuth_rad - panel_azimuth_rad))
    
    # Direct radiation component
    direct_tilt = direct_radiation * max(0, cos_incidence)
    
    # Diffuse radiation component (Perez model simplified)
    diffuse_tilt = diffuse_radiation * (1 + np.cos(panel_tilt_rad)) / 2
    
    # Ground-reflected radiation
    beam_horiz = direct_radiation * np.sin(solar_elevation_rad)
    global_horiz = beam_horiz + diffuse_radiation
    reflected = global_horiz * albedo * (1 - np.cos(panel_tilt_rad)) / 2
    
    # Total GTI
    gti = direct_tilt + diffuse_tilt + reflected
    
    return max(0, gti)

def fetch_historical_gti_data(latitude: float, longitude: float, 
                            start_date: str, end_date: str, 
                            panel_tilt: float = 30, 
                            panel_azimuth: float = 180,
                            timezone: str = 'Etc/UTC') -> pd.DataFrame:
    """
    Fetch historical radiation data and calculate GTI considering panel orientation
    
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
    panel_tilt : float
        Panel tilt angle from horizontal in degrees (default = 30)
    panel_azimuth : float
        Panel azimuth angle in degrees (default = 180, facing south)
    timezone : str
        Timezone identifier (Region/City)
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing hourly historical radiation data and calculated GTI
    """
    
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["direct_radiation", "diffuse_radiation", "shortwave_radiation"],
        "timezone": timezone
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        timezone = str(round((data.get('utc_offset_seconds', 0) / 3600)))
        
        # Convert API response to DataFrame
        df = pd.DataFrame({
            'timestamp': pd.to_datetime(data['hourly']['time']),
            'direct_radiation': data['hourly']['direct_radiation'],
            'diffuse_radiation': data['hourly']['diffuse_radiation'],
            'shortwave_radiation': data['hourly']['shortwave_radiation']
        })
        
        # Calculate solar position and GTI for each timestamp
        solar_positions = [calculate_solar_position(latitude, longitude, ts, timezone) 
                         for ts in df['timestamp']]
        
        df['solar_elevation'] = [pos[0] for pos in solar_positions]
        df['solar_azimuth'] = [pos[1] for pos in solar_positions]
        
        df['gti'] = df.apply(lambda row: calculate_gti(
            row['direct_radiation'],
            row['diffuse_radiation'],
            row['solar_elevation'],
            row['solar_azimuth'],
            panel_tilt,
            panel_azimuth
        ), axis=1)
        

        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def analyze_historical_gti(df: pd.DataFrame) -> dict:
    """
    Analyze historical GTI data and return key metrics
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing GTI data
        
    Returns:
    --------
    dict
        Dictionary containing various analysis metrics
    """
    if df is None or df.empty:
        return None
        
    analysis = {
        'daily_average_gti': df['gti'].resample('D').sum().mean(),  # W/m²/day
        'monthly_average_gti': df['gti'].resample('M').sum().mean(),  # W/m²/month
        'peak_gti': df['gti'].max(),  # W/m²
        'peak_gti_timestamp': df['gti'].idxmax(),
        'total_gti': df['gti'].sum(),  # W/m²
        'best_month': df['gti'].resample('M').sum().idxmax().strftime('%Y-%m'),
        'worst_month': df['gti'].resample('M').sum().idxmin().strftime('%Y-%m')
    }
    
    return analysis

# Example usage
if __name__ == "__main__":
    # Example coordinates (Denver)
    lat = 39.7392
    lon = -104.9903
    tz  = 'America/Denver'
    
    # Get historical data for the previous year
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    # Calculate GTI for a south-facing panel tilted at 30 degrees
    df = fetch_historical_gti_data(lat, lon, start_date, end_date, 
                                 panel_tilt=30, panel_azimuth=180,
                                 timezone = tz)
    
    if df is not None:
        print("\nSample of historical data:")
        print(df.head())
        
        print("\nMonthly GTI summaries (kWh/m²):")
        monthly_gti = df['gti'].resample('M').sum() / 1000
        print(monthly_gti)
        
        print("\nAnnual Analysis:")
        analysis = analyze_historical_gti(df)
        for key, value in analysis.items():
            print(f"{key}: {value}")



