#!/usr/bin/env python
# -*- coding: utf-8 -*- 

__author__ = 'Sunbeam Rahman'
__date__   = '11/01/2023 22:09:21'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ## 

import os
import xarray as xr
import pandas as pd
from glob import glob

input_dir = r"D:\Documents\RIMES\NETCDF\CanESM5"
output_dir = r"D:\Documents\RIMES\NETCDF\CanESM5\output"

for files in glob(input_dir + "/**/*.nc", recursive=True):

    file_name = os.path.basename(files)
    file_path = os.path.abspath(files)
    out_file = output_dir + "\\" + file_name[:-3] + ".csv"
    
    if os.path.isfile(file_path):

        if not file_name.endswith("csv") and not os.path.isfile(out_file):

            print("processing: " + file_path)

            ds = xr.open_dataset(file_path)
            # ds.to_dataframe().to_csv(out_file)

            # convert to pandas dataframe
            df = ds.to_dataframe()
            df = df.reset_index()

            df_col = [col for col in df.columns if col not in ['time', 'station', 'station_name']][0]

            # check if there is null values
            # print(df.isnull().sum())
            # print(df.isna().sum())

            # check avg tasmin for each month for each station
            # df_1 = df.groupby([df['time'].dt.year, df['time'].dt.month, 'station_name'])['tasmin'].mean()
            df_1 = df.groupby([df['time'].dt.year, 'station_name'])[df_col].mean()
            # convert series to dataframe
            df_1 = df_1.to_frame().reset_index()

            # pivot dataframe
            df_2 = df_1.pivot('station_name', 'time').stack(0).rename_axis(['station_name', 'value'])

            # convert all int Years into string
            for col in df_2.columns:
                df_2.rename(columns={col:'year_'+ str(col)},inplace=True)

            # finally export
            df_2.to_csv(out_file, index=True, header=True)
            
    