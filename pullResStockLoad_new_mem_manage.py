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

GAS_FUELS = {"natural_gas", "propane"}

def extract_building_id(path: str) -> str:
    """Extract the building ID from a filename."""
    return os.path.basename(path).removesuffix("-0.parquet").removesuffix(".parquet")


def load_state_metadata(state: str) -> pd.DataFrame:
    """Load metadata for a state."""
    path = f"s3://{PREFIX}state={state}/metadata.parquet"
    return pd.read_parquet(path, filesystem=fs)


def is_electric_only(meta: pd.DataFrame) -> pd.Series:
    """Return boolean mask for electric-only buildings."""
    return ~meta["heating_fuel"].isin(GAS_FUELS) & ~meta["cooking_fuel"].isin(GAS_FUELS)


def hourly_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate numeric columns to hourly resolution."""
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.floor("H")
    numeric_cols = df.select_dtypes("number").columns
    return df.groupby("timestamp", as_index=False)[numeric_cols].mean()


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

        numeric_cols = df.select_dtypes("number").columns
        df = df.groupby(["building_id", "timestamp"], as_index=False)[numeric_cols].mean()

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
    output_dir: str,
    batch_size: int = 100,
    max_workers: int = 8,
) -> List[Path]:
    """Process data files for a state, tracking progress in a manifest file."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = output_dir / f"{state}_manifest.json"
    manifest = load_manifest(manifest_path)

    metadata = load_state_metadata(state)
    electric_only_ids = set(metadata[is_electric_only(metadata)]["building_id"])

    file_paths = fs.glob(f"{PREFIX}state={state}/*.parquet")
    batches = list(batched(file_paths, batch_size))

    futures = {}
    output_paths = [Path(p) for p in manifest.values()]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, batch in enumerate(batches):
            if i in manifest:
                continue  # Already processed

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
