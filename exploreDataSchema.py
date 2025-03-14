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
import re


def list_files_from_openei_viewer(url, extension):
    """
    List available files of specified extension from OpenEI S3 viewer page that 
    are displayed in the UI.
    Parameters:
    url (str): URL of the OpenEI S3 viewer page
    Returns:
    list: List of URLs for files with specified extension
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
    
def list_paginated_files_from_openei_viewer(url, extension):
    
    """
    Loops through "Next" for pages with pagination and calls list_files_from_openei_viewer
    to get files from each page.
    Note that this is slow and inefficient for pages with lots of pagination.
    Parameters:
    url (str): URL of the OpenEI S3 viewer page
    Returns:
    list: List of URLs for CSV files
    """
    
    all_files = set()
    current_url = url
    
    while True:
        # Get files from current page
        files = list_files_from_openei_viewer(current_url, extension)
        all_files.update(files)
        
        # Find "Next" link
        response = requests.get(current_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        next_link = soup.find('a', text='Next') or soup.find('a', text=re.compile(r'Next'))
        
        if not next_link or not next_link.get('href'):
            break
            
            # previous_url value ensures function doesn't get stuck in a loop on the last page
        previous_url = current_url
        current_url = urljoin(url, next_link['href'])
        
        if previous_url == current_url:
            break
        
    return list(all_files)


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

def extract_file_info(urls):
    """
    Takes list of urls of files, gets the filename, and then extracts the 
    identifier. The identifier depends on the page.
    
    Parameters:
        urls (list): List of file urls.
    
    Returns:
        dictionary of
            dictionary: file urls
            filename: file names
            identifier: the identifier for the variable. 
    """
    
    # Main dictionary with URL as key
    files_by_url = {}
    
    # Additional lists to store all filenames and identifiers
    all_filenames = []
    all_identifiers = []
    
    for url in urls:
        # Extract filename from URL
        filename = url.split('/')[-1]
        
        # Extract identifier (everything before '-')
        identifier = filename.split('-')[0]
        
        # Store in the dictionary
        files_by_url[url] = {
            "filename": filename,
            "identifier": identifier
        }
        
        # Add to our lists
        all_filenames.append(filename)
        all_identifiers.append(identifier)
    
    return {
        "by_url": files_by_url,
        "filenames": all_filenames,
        "identifiers": all_identifiers
    }

###############################################################################
### Get meta data and annual results files ###
files = list_files_from_openei_viewer('https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=nrel-pds-building-stock%2Fend-use-load-profiles-for-us-building-stock%2F2024%2Fresstock_amy2018_release_2%2Fmetadata%2Fhttps://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=nrel-pds-building-stock%2Fend-use-load-profiles-for-us-building-stock%2F2024%2Fresstock_amy2018_release_2%2Fmetadata_and_annual_results%2Fby_state%2Fstate%3DCO%2Fparquet%2F',\
                                      '.parquet')

meta_data_annual_url = [file for file in files if file.endswith('baseline_metadata_and_annual_results.parquet')][0]

meta_data_annual_df = read_parquet_from_url(meta_data_annual_url)

###############################################################################
### Get data dictionary from tsv ###

url = 'https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_amy2018_release_2/data_dictionary.tsv'
data_dictionary_df = pd.read_csv(url, sep='\t')

###############################################################################
### Get enumeration dictionary from tsv ###

url = 'https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_amy2018_release_2/enumeration_dictionary.tsv'
enumeration_dictionary_df = pd.read_csv(url, sep='\t')


###############################################################################
### Get all Colorado building parquet files, and then extract corresponding building
#   IDs from the file name ###

files = list_paginated_files_from_openei_viewer('https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=nrel-pds-building-stock%2Fend-use-load-profiles-for-us-building-stock%2F2024%2Fresstock_amy2018_release_2%2Ftimeseries_individual_buildings%2Fby_state%2Fupgrade%3D0%2Fstate%3DCO%2F',\
                                      '.parquet')

urls = files

building_load_file_info = extract_file_info(urls)

###############################################################################
### Get all Colorado weather parquet files, and then extract corresponding building
#   IDs from the file name ###

