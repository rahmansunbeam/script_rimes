#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Sunbeam Rahman'
__date__   = '21/03/2023 17:12:04'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ##

import pandas as pd
import xarray as xr
import pathlib

# Define time intervals
interval_start_yr = 2021
interval_end_yr = 2100
interval_range_diff = 10
interval_delta = 29

intervals = [(pd.Timestamp(str(i) + '-01-01'), pd.Timestamp(str(i+interval_delta) + '-12-31')) for i in range(interval_start_yr, interval_end_yr, interval_range_diff) if i+interval_delta <= interval_end_yr]

input_dir = pathlib.Path(r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\historical\r1i1p1f1")
output_dir = input_dir / "output"
output_dir.mkdir(parents=True, exist_ok=True)
accumulated = 'sum' #'avg', 'sum'

for file in input_dir.glob("**\*.nc"):

    # # create output CSV filename
    # out_file = output_dir / (file.stem + ".csv")
    # create output JSON filename
    f_name = file.stem.replace('Bangladesh_southasia_', '').replace('index_', '').replace('_districts', '').replace('_divisions', '_div')
    # out_file = output_dir / (f_name + ".csv")
    out_file = output_dir / (f_name + ".json")

    if file.exists() and not out_file.exists():

        print("processing: {}".format(file))

        ds = xr.open_dataset(file)
        df = ds.to_dataframe().reset_index()
        df_col = [col for col in df.columns if col not in ['bnds', 'time_bnds', 'time', 'station', 'station_name']]

        # calculate interval averages for each station
        interval_dfs = []
        for interval in intervals:
            interval_start, interval_end = interval
            interval_mask = df['time'].between(interval_start, interval_end)
            interval_yearly_sum = df.loc[interval_mask].groupby(['station', 'station_name', df['time'].dt.year]).sum()[df_col]
            interval_yearly_mean = df.loc[interval_mask].groupby(['station', 'station_name', df['time'].dt.year]).mean()[df_col]

            if accumulated == 'sum':
                if file.parent.name == "pr":
                    interval_yearly_cal = interval_yearly_sum.groupby(['station', 'station_name']).sum()
                else:
                    interval_yearly_cal = interval_yearly_mean.groupby(['station', 'station_name']).mean()
            elif accumulated == 'avg':
                interval_yearly_cal = interval_yearly_mean.groupby(['station', 'station_name']).mean()
            else:
                print('error in accumulated param')
            interval_yearly_cal.columns = ['year_{}_{}'.format(interval_start.year, interval_end.year)]
            interval_dfs.append(interval_yearly_cal)

            # # 30-year mean
            # interval_df = df.loc[interval_mask, :].groupby(['station', 'station_name']).mean()[df_col]
            # interval_df.columns = ['year_{}_{}'.format(interval_start.year, interval_end.year)]
            # interval_dfs.append(interval_df)

        # merge interval averages into one dataframe
        merged_df = pd.concat(interval_dfs, axis=1)
        merged_df = merged_df.reset_index()

        # save dataframe to CSV
        # merged_df.to_csv(out_file, index=False, header=True)
        merged_df.to_json(out_file)
