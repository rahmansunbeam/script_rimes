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

input_dir = pathlib.Path(r"D:\Data\Bangladesh_CMIP6_sublevels\MIROC6\ssp585\r1i1p1f1")
output_dir = input_dir / "output"
output_dir.mkdir(parents=True, exist_ok=True)
accumulated = 'avg' #'avg', 'sum'
f_name_dfs = {}

for file in input_dir.glob("**\*.nc"):
    f_name = file.stem.replace('Bangladesh_', '').replace('index_', '').replace('Bangladesh_southasia_', '').replace('_districts', '').replace('_divisions', '_div')
    out_file = output_dir / (f_name + '_seasonal.json')

    if file.exists() and not out_file.exists():

        print("processing: {}".format(file))

        ds = xr.open_dataset(file)
        df = ds.to_dataframe().reset_index()
        df_col = [col for col in df.columns if col not in ['bnds', 'time_bnds', 'time', 'station', 'station_name']]

        seasonal_avgs = []
        # Calculate seasonal averages for each interval
        for interval in intervals:
            interval_start, interval_end = interval
            interval_mask = df['time'].between(interval_start, interval_end)

            for season, months in seasons:

                if not out_file.exists():
                    seasonal_df = df.loc[interval_mask & df['time'].dt.month.isin(months)]
                    # print("dim of {} for {}, {}-{} is {}".format(f_name, season, interval_start.year, interval_end.year, seasonal_df.shape))

                    if accumulated == 'sum':
                        if file.parent.name == "pr":
                            seasonal_cal = seasonal_df.groupby(['station']).sum()[df_col]
                        else:
                            seasonal_cal = seasonal_df.groupby(['station']).mean()[df_col]
                    elif accumulated == 'avg':
                        seasonal_cal = seasonal_df.groupby(['station']).mean()[df_col]
                    else:
                        print('error in accumulated param')

                    seasonal_cal.columns = ['year_{}_{}_{}'.format(season, interval_start.year, interval_end.year)]
                    seasonal_avgs.append(seasonal_cal)
                    # break

        # Store DataFrame in dict
        f_name_dfs[f_name] = pd.concat(seasonal_avgs, axis=1)

# Save merged DataFrames to JSON
for f_name, df in f_name_dfs.items():
    # out_file = output_dir / (f_name + '_seasonal.csv')
    out_file = output_dir / (f_name + '_seasonal.json')
    # df.to_csv(out_file, index=True, header=True)
    df.to_json(out_file)

