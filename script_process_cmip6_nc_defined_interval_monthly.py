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

# Define months
months = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

input_dir = pathlib.Path(r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\historical\r1i1p1f1")
output_dir = input_dir / "output"
output_dir.mkdir(parents=True, exist_ok=True)
accumulated = 'avg' #'avg', 'sum'
f_name_dfs = {}

for file in input_dir.glob("**\*.nc"):
    f_name = file.stem.replace('Bangladesh_southasia_', '').replace('index_', '').replace('_districts', '').replace('_divisions', '_div')
    out_file = output_dir / (f_name + '_monthly.json')
    # out_file = output_dir / (f_name + '_monthly.csv')

    if file.exists() and not out_file.exists():

        print("processing: {}".format(file))

        ds = xr.open_dataset(file)
        df = ds.to_dataframe().reset_index()
        df_col = [col for col in df.columns if col not in ['bnds', 'time_bnds', 'time', 'station', 'station_name']]
        monthly_avgs = []
        # Calculate monthly averages for each interval
        for interval in intervals:
            interval_start, interval_end = interval
            interval_mask = df['time'].between(interval_start, interval_end)

            for month, month_name in months.items():

                if not out_file.exists():
                    monthly_df = df.loc[interval_mask & (df['time'].dt.month == month)]
                    # print("dim of {} for {}, {}-{} is {}".format(f_name, month_name, interval_start.year, interval_end.year, monthly_df.shape))

                    if accumulated == 'sum':
                        if file.parent.name == "pr":
                            monthly_cal = monthly_df.groupby(['station']).sum()[df_col]
                        else:
                            monthly_cal = monthly_df.groupby(['station']).mean()[df_col]
                    elif accumulated == 'avg':
                        monthly_cal = monthly_df.groupby(['station']).mean()[df_col]
                    else:
                        print('error in accumulated param')

                    monthly_cal.columns = ['year_{}_{}_{}'.format(month_name, interval_start.year, interval_end.year)]
                    monthly_avgs.append(monthly_cal)

        # Store DataFrame in dict
        f_name_dfs[f_name] = pd.concat(monthly_avgs, axis=1)

# Save merged DataFrames to JSON
for f_name, df in f_name_dfs.items():
    # out_file = output_dir / (f_name + '_monthly.csv')
    out_file = output_dir / (f_name + '_monthly.json')
    # df.to_csv(out_file, index=True, header=True)
    df.to_json(out_file)

