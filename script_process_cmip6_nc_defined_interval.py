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
intervals = [(pd.Timestamp('2021-01-01'), pd.Timestamp('2050-12-31')),
             (pd.Timestamp('2031-01-01'), pd.Timestamp('2060-12-31')),
             (pd.Timestamp('2041-01-01'), pd.Timestamp('2070-12-31')),
             (pd.Timestamp('2051-01-01'), pd.Timestamp('2080-12-31')),
             (pd.Timestamp('2061-01-01'), pd.Timestamp('2090-12-31')),
             (pd.Timestamp('2071-01-01'), pd.Timestamp('2100-12-31'))]

input_dir = pathlib.Path(r"D:\DOCUMENTS\RIMES\NETCDF_CMIP6_sublevels\ACCESS-CM2\ssp585\r1i1p1f1")
output_dir = input_dir / "output"
output_dir.mkdir(parents=True, exist_ok=True)

for file in input_dir.glob("**\*.nc"):
    
    # create output CSV filename
    out_file = output_dir / (file.stem + ".csv")
    
    if file.exists() and not out_file.exists():
        
        print("processing: {}".format(file))

        ds = xr.open_dataset(file)
        df = ds.to_dataframe().reset_index()
        df_col = [col for col in df.columns if col not in ['time', 'station', 'station_name']]

        # calculate interval averages for each station
        interval_dfs = []
        for interval in intervals:
            interval_start, interval_end = interval
            interval_mask = df['time'].between(interval_start, interval_end)
            interval_df = df.loc[interval_mask, :].groupby(['station', 'station_name']).mean()[df_col]
            interval_df.columns = ['year_{}_{}'.format(interval_start.year, interval_end.year)]
            interval_dfs.append(interval_df)

        # merge interval averages into one dataframe
        merged_df = pd.concat(interval_dfs, axis=1)
        merged_df = merged_df.reset_index()

        # save dataframe to CSV
        merged_df.to_csv(out_file, index=True, header=True)
