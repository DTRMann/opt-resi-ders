# -*- coding: utf-8 -*-
"""
Created on Fri Sep 12 12:12:14 2025

@author: DTRManning
"""


from pathlib import Path
from collections import defaultdict
import pandas as pd
import numpy as np


data_folder = Path(r"C:\Users\DTRManning\Desktop\OptimizeResiGenSizing\data")


def load_parquets(folder: str, state: str) -> pd.DataFrame:
    """Load all parquet files into one DataFrame."""
    files = [f for f in Path(folder).glob("*.parquet") if f.name.startswith(state)]
    if not files:
        raise FileNotFoundError(f"No parquet files found in {folder}")
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    return df

def calc_net_load(df):
    """Replace source columns with a single 'net_load' column"""
    df["net_load_kwh"] = (
        df["out.electricity.net.energy_consumption"].to_numpy()
        - df["out.electricity.pv.energy_consumption"].to_numpy()
    )
    return df.drop(
        columns=[
            "out.electricity.net.energy_consumption",
            "out.electricity.pv.energy_consumption",
        ]
    )

def compute_outages(df: pd.DataFrame, windows=(1, 2, 4, 8)) -> dict:
    """
    Compute rolling outage kWh sums for each building_id.
    
    Returns:
        outages: dict[building_id][window] -> np.ndarray of rolling sums
    """
    # Ensure sorted by time
    df = df.sort_values(["building_id", "timestamp"])

    outages = defaultdict(dict)
    
    for bid, g in df.groupby("building_id"):
        values = g["net_load_kwh"].to_numpy()
        for w in windows:
            if len(values) < w: # Safeguard in case window is greater than time series
                outages[bid][w] = np.array([], dtype=float)
            else:
                rolled = np.convolve(values, np.ones(w, dtype=float), "valid") # Rolling sum via convolution 
                outages[bid][w] = rolled

    return outages
    

def flatten_by_window(outages: dict, window: int) -> dict:
    """
    Flatten all non-empty NumPy arrays for a given window,
    preserving building IDs. Returns a structured dictionary.
    """
    total_len = sum(
        outages[bid][window].size
        for bid in outages
        if window in outages[bid] and outages[bid][window].size > 0
    )

    if total_len == 0:
        return {"window": window, "data": np.array([]), "building_ids": np.array([])}

    sample_arr = next(
        (outages[bid][window] for bid in outages
         if window in outages[bid] and outages[bid][window].size > 0),
        None
    )
    
    # Pre-allocate for O(n)
    data_flat = np.empty(total_len, dtype=sample_arr.dtype)
    bid_flat = np.empty(total_len, dtype=object)

    pos = 0
    for bid, bid_data in outages.items():
        arr = bid_data.get(window, None)
        if arr is not None and arr.size > 0:
            n = arr.size
            data_flat[pos:pos+n] = arr
            bid_flat[pos:pos+n] = bid
            pos += n

    return {"window": window, "data": data_flat, "building_ids": bid_flat}

# Testing
state = "CO"
df = load_parquets(data_folder, state)
df = calc_net_load(df)
outages = compute_outages(df, windows=(1, 2, 4, 8))

# Get distribution by window
all_four_hour = flatten_by_window(outages, 4)


