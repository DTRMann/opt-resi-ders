# -*- coding: utf-8 -*-
"""
Created on Fri Jul 11 21:16:15 2025

@author: DTRManning
"""
import os
from pathlib import Path
from typing import List, Iterator
import json

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
import s3fs

# Modules
import getStateMetaData

# anonymous S3 file system
fs = s3fs.S3FileSystem(anon=True)

# Constants
read_cols = [ 'timestamp', 'out.electricity.net.energy_consumption' ]
supported_energy = ['Electric', 'Electric Resistance', 'Electric Induction',
                    'Electricity'] # Allowed energy sources; assuming all electric homes

# Base S3 bucket prefix for the 2024.2 ResStock raw timeseries
PREFIX = (
    "oedi-data-lake/nrel-pds-building-stock/"
    "end-use-load-profiles-for-us-building-stock/"
    "2024/resstock_amy2018_release_2/"
    "timeseries_individual_buildings/by_state/"
    "upgrade=0/"
)

def extract_building_id(path: str) -> str:
    """Extract the building ID from a filename."""
    return os.path.basename(path).removesuffix("-0.parquet").removesuffix(".parquet")


def is_electric_only(state: str, supported_energy: List[str]) -> pd.Series:
    """Returns electric only homes for a given state."""
    state_electric_meta = getStateMetaData.fetch_state_metadata_df(state)
    all_electric_bldgs  = state_electric_meta[
        (state_electric_meta['in.clothes_dryer'].isin(supported_energy)) &
        (state_electric_meta['in.cooking_range'].isin(supported_energy)) &
        (state_electric_meta['in.heating_fuel'].isin(supported_energy)) &
        (state_electric_meta['in.water_heater_fuel'].isin(supported_energy))]
    all_electric_bldgs = all_electric_bldgs.index.astype(str) 
    return all_electric_bldgs


def hourly_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate numeric columns to hourly resolution."""
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.floor("H")
    numeric_cols = df.select_dtypes("number").columns
    return df.groupby("timestamp", as_index=False)[numeric_cols].sum()


def read_batch(batch_paths: List[str], allowed_ids: set[str], columns: List[str]) -> pd.DataFrame:
    """Read and process a batch of parquet files."""
    frames = []

    for path in batch_paths:
        building_id = extract_building_id(path)
        if building_id not in allowed_ids:
            continue

        df = pd.read_parquet(f"s3://{path}", filesystem=fs, columns=columns)
        df["building_id"] = building_id
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.floor("H")

        df = hourly_aggregate(df)

        frames.append(df)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def batched(iterable: List[str], batch_size: int) -> Iterator[List[str]]:
    """Yield successive batches from a list."""
    it = iter(iterable)
    while batch := list(islice(it, batch_size)):
        yield batch


def process_state_in_batches(
    state: str,
    columns: List[str],
    supported_energy: List[str],
    output_dir: str,
    batch_size: int = 100,
    max_workers: int = 8,
) -> List[Path]:
    """Process data files for a state, tracking progress in a manifest file."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = output_dir / f"{state}_manifest.json"
    manifest = load_manifest(manifest_path)

    electric_only_ids = is_electric_only(state, supported_energy)
    
    # TODO - subset to electric only

    file_paths = fs.glob(f"{PREFIX}state={state}/*.parquet")
    batches = list(batched(file_paths, batch_size))

    futures = {}
    output_paths = [Path(p) for p in manifest.values()]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, batch in enumerate(batches):
            if i in manifest:
                continue  # Path already processed

            output_file = output_dir / f"{state}_batch_{i:03}.parquet"
            futures[executor.submit(read_batch, batch, electric_only_ids, columns)] = (i, output_file)

        for future in as_completed(futures):
            i, output_file = futures[future]
            try:
                result_df = future.result()
                if not result_df.empty:
                    result_df.to_parquet(output_file, index=False)
                    manifest[i] = str(output_file)
                    output_paths.append(output_file)
                    save_manifest(manifest, manifest_path)
            except Exception as e:
                print(f"Error processing batch {i}: {e}")

    return output_paths

def load_manifest(manifest_path: Path) -> dict[int, str]:
    if manifest_path.exists():
        with open(manifest_path, "r") as f:
            return {int(k): v for k, v in json.load(f).items()}
    return {}

def save_manifest(manifest: dict[int, str], manifest_path: Path) -> None:
    # Sort keys for readability
    with open(manifest_path, "w") as f:
        json.dump({str(k): v for k, v in sorted(manifest.items())}, f, indent=2)

# For testing
#data_paths = process_state_in_batches(state = 'CO', 
#                               columns = read_cols,
#                               supported_energy = supported_energy,
#                               output_dir = r"C:\Users\DTRManning\Desktop\OptimizeResiGenSizing\data",
#                               batch_size = 5,
#                               max_workers = 5 )
