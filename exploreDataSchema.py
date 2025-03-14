# -*- coding: utf-8 -*-
"""
Created on Wed Mar 12 21:34:46 2025

@author: DTRManning
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import sys
from io import StringIO, BytesIO
import pandas as pd
import os


def list_files_from_openei_viewer(url, extension):
    """
    List available CSV files from OpenEI S3 viewer page that are displayed in the UI.
    Parameters:
    url (str): URL of the OpenEI S3 viewer page
    Returns:
    list: List of URLs for CSV files
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')
        
        if not table:
            print(f"No table found on page: {url}", file=sys.stderr)
            return []
            
        # Use a set to automatically eliminate duplicates
        file_links_set = set()
        
        # Find only visible rows within the table
        rows = table.find_all('tr', style=lambda value: not value or 'display: none' not in value)
        for row in rows:
            links = row.find_all('a', href=True)
            for link in links:
                href = link['href']
                if href.lower().endswith(extension):
                    absolute_url = urljoin(url, href)
                    file_links_set.add(absolute_url)
                    
        return list(file_links_set)
    except Exception as e:
        print(f"Error fetching files from {url}: {e}", file=sys.stderr)
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
    Reads the CSV file for the given state code from the OpenEI data repository.
    
    Parameters:
        state_code (str): The two-letter state code (e.g., 'CA' for California).
    
    Returns:
        pd.DataFrame: The loaded data as a pandas DataFrame.
    """
    base_url = f"https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_amy2018_release_2/metadata_and_annual_results/by_state/state%3D{state_code}/csv/"
    file_name = f"{state_code}_baseline_metadata_and_annual_results.csv"
    file_url = base_url + file_name
    
    response = requests.get(file_url)
    if response.status_code == 200:
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)
        return df
    else:
        raise ValueError(f"Failed to fetch data for state {state_code}. HTTP Status: {response.status_code}")



### Build function to read a specified parquet file
        
def read_parquet_from_url(url):
    """
    Read a parquet file from a URL into a pandas DataFrame using pyarrow engine.
    
    Parameters:
    -----------
    url : str
        The URL of the parquet file to read.
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing the data from the parquet file.
        
    Raises:
    -------
    ValueError
        If the URL is invalid or if the file cannot be read as a parquet file.
    """
    try:
        # Validate URL
        parsed_url = urlparse(url)
        if not parsed_url.scheme or not parsed_url.netloc:
            raise ValueError(f"Invalid URL: {url}")
        
        # Download the file content
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Read the parquet file from the binary content using pyarrow engine
        buffer = BytesIO(response.content)
        df = pd.read_parquet(buffer, engine='pyarrow')
        
        print(f"Successfully read parquet file from {url}")
        print(f"DataFrame shape: {df.shape[0]} rows × {df.shape[1]} columns")
        
        return df
    
    except requests.exceptions.RequestException as e:
        raise ValueError(f"Error downloading file from URL: {e}")
    except Exception as e:
        raise ValueError(f"Error reading parquet file: {e}")



### Get meta data files
files = list_files_from_openei_viewer('https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=nrel-pds-building-stock%2Fend-use-load-profiles-for-us-building-stock%2F2024%2Fresstock_amy2018_release_2%2Fmetadata%2F',\
                                      '.parquet')
      
# Pull out baseline data to start 
[file for file in files if file.endswith('baseline.parquet')]
        

