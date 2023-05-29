#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Sunbeam Rahman'
__date__   = '28/05/2023 11:21:06'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ##

import pandas as pd
import xarray as xr
import pathlib

# Define time intervals
intervals = [(pd.Timestamp('2021-01-01'), pd.Timestamp('2050-12-31')),
             (pd.Timestamp('2031-01-01'), pd.Timestamp('2060-12-31')),
             (pd.Timestamp('2041-01-01'), pd.Timestamp('2070-12-31')),
             (pd.Timestamp('2051-01-01'), pd.Timestamp('2080-12-31')),
             (pd.Timestamp('2061-01-01'), pd.Timestamp('2090-12-31')),
             (pd.Timestamp('2071-01-01'), pd.Timestamp('2100-12-31'))]

# Define seasons
seasons = [('DJF', [12, 1, 2]), ('MAM', [3, 4, 5]), ('JJA', [6, 7, 8]), ('SON', [9, 10, 11])]

input_dir = pathlib.Path(r"D:\Data\CMIP6\ACCESS-CM2\ssp245")
output_dir = input_dir / "output"
output_dir.mkdir(parents=True, exist_ok=True)

f_name_dfs = {}

for file in input_dir.glob("**\*.nc"):
    f_name = file.stem.replace('Bangladesh_southasia_', '').replace('_districts', '').replace('_divisions', '_div')
    out_file = output_dir / (f_name + '_seasonal.json')

    if file.exists():
        ds = xr.open_dataset(file)
        df = ds.to_dataframe().reset_index()
        df_col = [col for col in df.columns if col not in ['time', 'station', 'station_name']]

        seasonal_avgs = []
        # Calculate seasonal averages for each interval
        for interval in intervals:
            interval_start, interval_end = interval
            interval_mask = df['time'].between(interval_start, interval_end)

            for season, months in seasons:

                if not out_file.exists():
                    seasonal_df = df.loc[interval_mask & df['time'].dt.month.isin(months)]
                    seasonal_avg = seasonal_df.groupby(['station', 'station_name']).mean()[df_col]
                    seasonal_avg.columns = ['year_{}_{}_{}'.format(season, interval_start.year, interval_end.year)]
                    seasonal_avgs.append(seasonal_avg)
                    # break

        # Store DataFrame in dict
        f_name_dfs[f_name] = pd.concat(seasonal_avgs, axis=1)

# Save merged DataFrames to JSON
for f_name, df in f_name_dfs.items():    
    df.to_json(out_file)

