# -*- coding: utf-8 -*-
"""
Created on Mon Jun 30 21:59:13 2025

@author: DTRMann
"""

import pandas as pd
import s3fs
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# anonymous S3 file system
fs = s3fs.S3FileSystem(anon=True)

# Base S3 bucket prefix for the 2024.2 ResStock raw timeseries
PREFIX = (
    "oedi-data-lake/nrel-pds-building-stock/"
    "end-use-load-profiles-for-us-building-stock/"
    "2024/resstock_amy2018_release_2/"
    "timeseries_individual_buildings/by_state/"
    "upgrade=0/"
)

STATES = ['CO']
#STATES = [
#    "AL","AR","AZ","CA","CO","CT","DC","DE","FL","GA","IA","ID","IL","IN","KS",
#    "KY","LA","MA","MD","ME","MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ",
#    "NM","NV","NY","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VA","VT",
#    "WA","WI","WV","WY",
#]

def extract_building_id(path: str) -> str:
    """
    Extract building ID from the parquet filename.
    """
    filename = os.path.basename(path)
    # drop the "-0.parquet" suffix
    if filename.endswith("-0.parquet"):
        return filename[: -len("-0.parquet")]
    # fallback if pattern changes
    return filename.rsplit(".", 1)[0]

def read_parquet_with_meta(path: str, columns: list[str]) -> pd.DataFrame:
    """Read parquet file with column subsetting, attach building_id."""
    df = pd.read_parquet(f"s3://{path}", filesystem=fs, columns=columns)
    df["building_id"] = extract_building_id(path)
    return df

def fetch_state_raw(state: str, columns: list[str], max_workers: int = 8) -> pd.DataFrame:
    """
    Download parquet files for a state, project selected columns,
    and tag with building_id extracted from filename.
    """
    path_pattern = PREFIX + f"state={state}/*.parquet"
    parts = fs.glob(path_pattern)

    if not parts:
        raise ValueError(f"No files found for state={state}")
    
    total = len(parts)
    print(f"[{state}] Fetching {total} files with columns: {columns}")

    dfs = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_path = {
            executor.submit(read_parquet_with_meta, path, columns): path
            for path in parts
        }
        for i, future in enumerate(as_completed(future_to_path), 1):
            try:
                df_part = future.result()
                dfs.append(df_part)
                # efficient overwrite progress
                if i % 10 == 0 or i == total:
                    print(f"\r[{state}] Completed {i}/{total}", end="", flush=True)
            except Exception as e:
                print(f"\nError reading {future_to_path[future]}: {e}")

    print()  # newline after progress
    df = pd.concat(dfs, ignore_index=True)
    df["state"] = state
    return df

# Single state example for dev
state = 'CO'

read_cols = [ 'timestamp', 'out.electricity.net.energy_consumption', 
             'out.electricity.total.energy_consumption', 
             'out.electricity.pv.energy_consumption']


data_co = fetch_state_raw( state, read_cols )

# iterate all states
#all_states = pd.concat((fetch_state_raw(s) for s in STATES), ignore_index=True)

# remap to your schema
#all_states = all_states.rename(columns=COLUMN_MAP)

# now `all_states` is long‐form: one row per building × timestamp,
# with your mapped columns plus “state”
# e.g. save to disk or feed into your ML pipeline:
#all_states.to_parquet("resstock_raw_by_state.parquet", index=False)

