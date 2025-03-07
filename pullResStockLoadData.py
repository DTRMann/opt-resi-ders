# -*- coding: utf-8 -*-
"""
Created on Fri Mar  7 09:01:16 2025

@author: DTRManning
"""

# Data source and documentation: https://resstock.nrel.gov/datasets
# Detailed hourly parquet data


import pandas as pd
import numpy as np
import os
import sys
import requests
from urllib.parse import urljoin
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
from io import StringIO


def load_csv_from_url(url, output_file=None, sep=','):
    """
    Load a CSV file from a URL into a pandas DataFrame with error handling.
    
    Parameters:
    url (str): URL of the CSV file to download
    output_file (str, optional): Path to save the DataFrame as CSV
    sep (str, optional): Separator used in the file, default is comma
    
    Returns:
    pandas.DataFrame or None: DataFrame containing the CSV data if successful, None otherwise
    """
    try:
        print(f"Downloading data from: {url}")
        # Read CSV directly from URL
        df = pd.read_csv(url, sep=sep)
        
        # Save to file if specified
        if output_file:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            df.to_csv(output_file, index=False)
            print(f"Data successfully saved to {output_file}")
        
        return df
    
    except pd.errors.ParserError:
        print(f"Error: Failed to parse the file at {url}. The file may be corrupted or in an unexpected format.", 
              file=sys.stderr)
        return None
    except pd.errors.EmptyDataError:
        print(f"Error: The file at {url} is empty.", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error: Failed to load the file from {url}: {str(e)}", file=sys.stderr)
        return None

def list_files_from_openei_viewer(url):
    """
    List available CSV files from OpenEI S3 viewer page that are displayed in the UI.

    Parameters:
    url (str): URL of the OpenEI S3 viewer page

    Returns:
    list: List of URLs for CSV files
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors

        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the main table where files are displayed
        table = soup.find('table')  # Adjust this if the table has a specific class or id
        
        if not table:
            print(f"No table found on page: {url}", file=sys.stderr)
            return []

        file_links = []

        # Find only visible rows within the table
        rows = table.find_all('tr', style=lambda value: not value or 'display: none' not in value)

        for row in rows:
            links = row.find_all('a', href=True)
            for link in links:
                href = link['href']

                # Ensure it's a visible .csv link
                if href.lower().endswith('.csv'):
                    absolute_url = urljoin(url, href)
                    file_links.append(absolute_url)

        return file_links

    except requests.exceptions.RequestException as e:
        print(f"Error fetching file list from {url}: {e}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"Error parsing file list from {url}: {e}", file=sys.stderr)
        return []


def identify_weather_data_urls(state_code='CO'):
    """
    Download and process weather data for specified state
    
    Parameters:
    state_code (str): State code (e.g., 'CO' for Colorado)
    
    Returns:
    list or None: list of weather data paths if successful, None otherwise
    """
    # Weather data URL
    weather_url = f"https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=nrel-pds-building-stock%2Fend-use-load-profiles-for-us-building-stock%2F2024%2Fresstock_amy2018_release_2%2Fweather%2Fstate%3D{state_code}%2F"
    
    # List available files
    file_urls = list_files_from_openei_viewer(weather_url)
    
    if not file_urls:
        print(f"No weather files found for state {state_code}")
        return None
    
    print(f"Found {len(file_urls)} weather files for {state_code}")
    
    # Filter for CSV files
    csv_urls = [url for url in file_urls if url.endswith('.csv')]
    if not csv_urls:
        print(f"No CSV weather files found for state {state_code}")
        return None
    
    return file_urls

def read_state_meta_data(state_code):
    """
    Reads the meta data for the given state code from the OpenEI data repository.
    
    Parameters:
        state_code (str): The two-letter state code (e.g., 'CA' for California).
    
    Returns:
        pd.DataFrame: The loaded data as a pandas DataFrame.
    """
    base_url = "https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=nrel-pds-building-stock%2Fend-use-load-profiles-for-us-building-stock%2F2024%2Fresstock_amy2018_release_2%2Fmetadata_and_annual_results%2Fby_state%2Fstate%3D"
    file_name = f"{state_code}%2F{state_code}_baseline_metadata_and_annual_results.csv"
    file_url = base_url + file_name
    
    response = requests.get(file_url)
    if response.status_code == 200:
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)
        return df
    else:
        raise ValueError(f"Failed to fetch data for state {state_code}. HTTP Status: {response.status_code}")


def merge_datasets(weather_data, load_data, state_code='CO'):
    """
    Merge weather and load datasets based on common identifiers
    
    Parameters:
    weather_data (pandas.DataFrame): Weather data
    load_data (pandas.DataFrame): Load data
    state_code (str): State code for output file naming
    
    Returns:
    pandas.DataFrame or None: Merged data if successful, None otherwise
    """
    if weather_data is None or load_data is None:
        print("Can't merge datasets: one or both datasets are missing")
        return None
    
    # Examine the datasets to identify common keys for merging
    print("\nWeather data potential join keys:")
    print(weather_data.columns[weather_data.columns.str.contains('id|ID|building|location|epw')].tolist())
    
    print("\nLoad data potential join keys:")
    print(load_data.columns[load_data.columns.str.contains('id|ID|building|location|epw')].tolist())
    
    # Check for exact matches in column names
    common_columns = set(weather_data.columns) & set(load_data.columns)
    print(f"\nCommon columns in both datasets: {common_columns}")
    
    # Save both datasets to CSV for manual examination if needed
    os.makedirs(f'data/{state_code}', exist_ok=True)
    weather_data.to_csv(f'data/{state_code}/weather_sample.csv', index=False)
    load_data.to_csv(f'data/{state_code}/load_sample.csv', index=False)
    
    # Try to find a common key for merging
    merge_key = None
    for key in ['building_id', 'building', 'id', 'location_id', 'epw_id']:
        if key in weather_data.columns and key in load_data.columns:
            merge_key = key
            break
    
    if merge_key:
        print(f"\nFound common key: {merge_key}")
        try:
            merged_data = pd.merge(load_data, weather_data, on=merge_key, how='inner')
            print(f"Successfully merged data on {merge_key}. Merged shape: {merged_data.shape}")
            
            # Save the merged dataset
            merged_data.to_csv(f'data/{state_code}/merged_data.csv', index=False)
            return merged_data
        except Exception as e:
            print(f"Error merging datasets: {e}")
    else:
        print("\nCould not identify a common key for merging.")
        print("The datasets have been saved for manual examination.")
    
    return None

def basic_analysis(merged_data, state_code='CO', weather_data=None, load_data=None):
    """
    Perform basic analysis on the data
    
    Parameters:
    merged_data (pandas.DataFrame): Merged dataset (can be None)
    state_code (str): State code for output file naming
    weather_data (pandas.DataFrame): Weather data (used if merged_data is None)
    load_data (pandas.DataFrame): Load data (used if merged_data is None)
    """
    # Create output directory
    os.makedirs(f'data/{state_code}', exist_ok=True)
    
    if merged_data is None:
        if weather_data is not None and load_data is not None:
            print("Performing separate analysis on weather and load data...")
            
            # Basic weather data analysis
            if 'dry_bulb_temperature' in weather_data.columns:
                print("\nWeather temperature statistics:")
                print(weather_data['dry_bulb_temperature'].describe())
                
                # Create temperature distribution plot
                plt.figure(figsize=(10, 6))
                weather_data['dry_bulb_temperature'].hist(bins=50)
                plt.title(f'Temperature Distribution for {state_code}')
                plt.xlabel('Temperature (°F)')
                plt.ylabel('Frequency')
                plt.savefig(f'data/{state_code}/temperature_dist.png')
                plt.close()
                print(f"Saved temperature distribution chart to data/{state_code}/temperature_dist.png")
            
            # Basic load data analysis
            if 'total_site_electricity_kwh' in load_data.columns:
                print("\nElectricity usage statistics:")
                print(load_data['total_site_electricity_kwh'].describe())
                
                # Create electricity usage distribution plot
                plt.figure(figsize=(10, 6))
                load_data['total_site_electricity_kwh'].hist(bins=50)
                plt.title(f'Electricity Usage Distribution for {state_code}')
                plt.xlabel('Electricity Usage (kWh)')
                plt.ylabel('Frequency')
                plt.savefig(f'data/{state_code}/electricity_dist.png')
                plt.close()
                print(f"Saved electricity distribution chart to data/{state_code}/electricity_dist.png")
        else:
            print("No data available for analysis")
        return
    
    # If we have merged data, perform analysis on the combined dataset
    print("\nPerforming analysis on the merged dataset...")
    
    # Show correlations between key weather and load variables
    weather_vars = ['dry_bulb_temperature', 'relative_humidity', 'wind_speed'] 
    weather_vars = [var for var in weather_vars if var in merged_data.columns]
    
    load_vars = ['total_site_electricity_kwh', 'heating_electricity_kwh', 'cooling_electricity_kwh']
    load_vars = [var for var in load_vars if var in merged_data.columns]
    
    if weather_vars and load_vars:
        print("\nCorrelation between weather and load variables:")
        correlation = merged_data[weather_vars + load_vars].corr()
        print(correlation)
        
        # Save correlation matrix
        plt.figure(figsize=(12, 8))
        plt.matshow(correlation, fignum=1)
        plt.xticks(range(len(correlation.columns)), correlation.columns, rotation=90)
        plt.yticks(range(len(correlation.columns)), correlation.columns)
        plt.colorbar()
        plt.title(f'Correlation Matrix for {state_code}')
        plt.savefig(f'data/{state_code}/correlation_matrix.png')
        plt.close()
        print(f"Saved correlation matrix to data/{state_code}/correlation_matrix.png")

def process_resstock_data(state_code='CO', max_files=1):
    """
    Process ResStock data for a specific state
    
    Parameters:
    state_code (str): State code (e.g., 'CO' for Colorado)
    max_files (int): Maximum number of files to download per category
    
    Returns:
    tuple: (weather_data, load_data, merged_data) - any can be None if processing failed
    """
    print(f"Starting NREL ResStock data processing for state: {state_code}")
    
    state_code = 'CO'
    
    # Download meta data file
    state_meta_data = read_state_meta_data(state_code)
        
    # Identify weather data URLs
    weather_data_urls = identify_weather_data_urls(state_code)
    
    # TODO - figure out how to parse load data
    
    # TODO - figure out how to match weather and load data
    
    # Merge datasets
    merged_data = merge_datasets(weather_data, load_data, state_code)
    
    # Perform basic analysis
    basic_analysis(merged_data, state_code, weather_data, load_data)
    
    print(f"\nProcessing complete for {state_code}!")
    print(f"Data files saved in data/{state_code}/ directory.")
    
    return weather_data, load_data, merged_data

# Example usage
if __name__ == "__main__":
    
    # Process data for Colorado (default)
    weather_data, load_data, merged_data = process_resstock_data()
    
    # Example: Process data for another state
    # weather_data_ca, load_data_ca, merged_data_ca = process_resstock_data('CA', max_files=2)
