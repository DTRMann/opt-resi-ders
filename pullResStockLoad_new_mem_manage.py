# -*- coding: utf-8 -*-
"""
Created on Fri Jul 11 21:16:15 2025

@author: DTRManning
"""
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
from pathlib import Path
import s3fs

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

def extract_building_id(path: str) -> str:
    """Extract building ID from the file name (before '-0.parquet')."""
    name = os.path.basename(path)
    return name.removesuffix("-0.parquet").removesuffix(".parquet")

def hour_floor(df: pd.DataFrame, time_col: str) -> pd.Series:
    """Floor timestamps to the hour."""
    return pd.to_datetime(df[time_col]).dt.floor("H")

def hourly_aggregate(df: pd.DataFrame, time_col: str = "timestamp") -> pd.DataFrame:
    """Aggregate numeric columns by hour and building."""
    df[time_col] = hour_floor(df, time_col)
    group_keys = ["building_id", time_col]
    numeric = df.select_dtypes("number").columns.tolist()
    return df.groupby(group_keys, as_index=False)[numeric].sum()

def read_aggregate_write(path: str, columns: List[str], output_dir: Path) -> Path:
    """Read one parquet file, aggregate to hourly, write result."""
    building_id = extract_building_id(path)

    df = pd.read_parquet(f"s3://{path}", filesystem=fs, columns=columns)
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.floor("H")
    
    numeric_cols = df.select_dtypes("number").columns.tolist()
    df_hourly = df.groupby("timestamp", as_index=False)[numeric_cols].sum()

    output_path = output_dir / f"{building_id}_hourly.parquet"
    df_hourly.to_parquet(output_path, index=False)
    return output_path

def fetch_state_hourly(
    state: str,
    columns: List[str],
    output_dir: str,
    max_workers: int = 8,
) -> List[Path]:
    """Process all parquet files for a state: hourly aggregate and write per-building results."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    pattern = f"{PREFIX}state={state}/*.parquet"
    files = fs.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No files found for state '{state}'")

    print(f"[{state}] Processing {len(files)} files...")

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(read_aggregate_write, path, columns, output_dir): path
            for path in files
        }

        results = []
        for i, future in enumerate(as_completed(futures), 1):
            path = futures[future]
            try:
                result_path = future.result()
                results.append(result_path)
                if i % 10 == 0 or i == len(futures):
                    print(f"\r[{state}] Done {i}/{len(futures)}", end="", flush=True)
            except Exception as e:
                print(f"\n[{state}] Failed on {path}: {e}")
    print()
    return results


