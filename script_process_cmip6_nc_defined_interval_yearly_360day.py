#!/usr/bin/env python
# -*- coding: utf-8 -*- 

__author__ = 'Sunbeam Rahman'
__date__   = '21/03/2023 17:12:04'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ## 

import pandas as pd
import pathlib
from cftime import Datetime360Day

# Define time intervals
intervals = [
    (Datetime360Day(2021, 1, 1), Datetime360Day(2050, 12, 30)),
    (Datetime360Day(2031, 1, 1), Datetime360Day(2060, 12, 30)),
    (Datetime360Day(2041, 1, 1), Datetime360Day(2070, 12, 30)),
    (Datetime360Day(2051, 1, 1), Datetime360Day(2080, 12, 30)),
    (Datetime360Day(2061, 1, 1), Datetime360Day(2090, 12, 30)),
    (Datetime360Day(2071, 1, 1), Datetime360Day(2100, 12, 30))
]

# Define directories
input_dir = pathlib.Path(r"D:\Data\CLIMDATA_MAIN\UKESM1-0-LL\output")
output_dir = input_dir / "output"
output_dir.mkdir(parents=True, exist_ok=True)

# Process each CSV file in the directory
for file in input_dir.glob("*daily_ssp245.csv"):
    # Define output file name
    out_file = output_dir / (file.stem + "_avg.csv")

    if not out_file.exists():
        df = pd.read_csv(file)

        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['date'] = df['date'].apply(lambda x: Datetime360Day(x.year, x.month, min(x.day, 30)) if pd.notnull(x) else pd.NaT)

        interval_dfs = []

        for interval_start, interval_end in intervals:
            mask = (df['date'] >= interval_start) & (df['date'] <= interval_end)

            interval_df = df.loc[mask].groupby(['station', 'station_name']).mean()
            interval_df.columns = ['avg_{}_{}'.format(interval_start.year, interval_end.year)]

            interval_dfs.append(interval_df)

        # Concatenate all interval dataframes
        merged_df = pd.concat(interval_dfs, axis=1).reset_index()

        # Save the result to the output file
        merged_df.to_csv(out_file, index=False)

print("Processing completed.")
