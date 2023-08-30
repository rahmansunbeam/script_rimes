#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Sunbeam Rahman'
__date__   = '28/05/2023 11:21:06'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ##

import pandas as pd
import xarray as xr
import pathlib

# # these interval ranges are for calculating projected data only
# interval_start_yr = 2021
# interval_end_yr = 2100
# interval_range_diff = 10
# interval_delta = 29

# # these interval are for calculating diff with historical data (1950-2014) with projected data (2015-2100)
interval_start_yr = 1950
interval_end_yr = 2014
interval_range_diff = 5
interval_delta = 29

intervals = [(pd.Timestamp(str(i) + '-01-01'), pd.Timestamp(str(i+interval_delta) + '-12-31')) for i in range(interval_start_yr, interval_end_yr, interval_range_diff) if i+interval_delta <= interval_end_yr]

# # Define months
months = {i: pd.Timestamp(str(i) + '-01-01').strftime('%b') for i in range(1, 13)}

input_dir = pathlib.Path(r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\historical\r1i1p1f1")
output_dir = input_dir / "output"
output_dir.mkdir(parents=True, exist_ok=True)
accumulated = 'sum' #'avg', 'sum'
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

                            # for the everything else diff interval, th pr is summed
                            # monthly_cal = monthly_df.groupby(['station']).sum()[df_col]

                            # for diff interval, the pr files are yearly averaged
                            monthly_cal = monthly_df.groupby(['station']).sum()[df_col]/30

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

