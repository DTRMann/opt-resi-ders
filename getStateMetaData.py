# -*- coding: utf-8 -*-
"""
Created on Fri Jul 11 11:39:58 2025

@author: DTRManning
"""

from io import BytesIO
import requests
import pandas as pd


BASE_S3_URL = "https://oedi-data-lake.s3.amazonaws.com"
BUCKET_SUBPATH = (
    "nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/"
    "2024/resstock_amy2018_release_2/metadata_and_annual_results/by_state"
)


def get_download_url(state: str) -> str:
    """
    Construct the full S3 URL for the given state, using the
    `state=<ABBR>/parquet/` folder structure.
    """
    state = state.upper()
    filename = f"{state}_baseline_metadata_and_annual_results.parquet"
    return (
        f"{BASE_S3_URL}/{BUCKET_SUBPATH}/state%3D{state}/"
        f"parquet/{filename}"
    )


def fetch_state_metadata_df(state: str) -> pd.DataFrame:
    """
    Fetch the ResStock metadata + annual results as a pandas DataFrame
    for the specified two-letter state abbreviation.
    
    Raises:
        RuntimeError on HTTP or connection issues.
    """
    url = get_download_url(state)
    try:
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
    except requests.HTTPError as err:
        raise RuntimeError(f"HTTP error fetching {url}: {err}") from err
    except requests.RequestException as err:
        raise RuntimeError(f"Connection error fetching {url}: {err}") from err

    # Read directly into a DataFrame
    buffer = BytesIO(resp.content)
    df = pd.read_parquet(buffer)
    return df


meta_data_df = fetch_state_metadata_df('CO')




