# -*- coding: utf-8 -*-
"""
Created on Fri Jul 11 11:24:31 2025

@author: DTRManning
"""

import pandas as pd

# load the live dictionary for current release & weather-year
url = (
    "https://oedi-data-lake.s3.amazonaws.com/"
    "nrel-pds-building-stock/"
    "end-use-load-profiles-for-us-building-stock/2024/"
    "resstock_amy2018_release_2/"
    "data_dictionary.tsv"
)
dict_df = pd.read_csv(url, sep="\t")