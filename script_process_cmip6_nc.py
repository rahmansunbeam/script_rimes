#!/usr/bin/env python
# -*- coding: utf-8 -*- 

__author__ = 'Sunbeam Rahman'
__date__   = '11/01/2023 22:09:21'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ## 

import pathlib
import xarray as xr
import numpy as np

input_dir = pathlib.Path(r"D:\Data\ACCESS-CM2\ssp585\r1i1p1f1\tas\month")

for file in input_dir.glob("**\*.nc"):
    
    # check if the folder exists, create if not
    output_dir = input_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    out_file = output_dir / (file.stem + ".csv")
    
    if file.exists():

        if not out_file.exists():

            print("processing: {}".format(file))

            ds = xr.open_dataset(file)

            # convert to pandas dataframe
            df = ds.to_dataframe()
            df = df.reset_index()

            df_col = [col for col in df.columns if col not in ['time', 'station', 'station_name']][0]
            # df_col = [col for col in df.columns if col not in ['time', 'bnds', 'station', 'time_bnds', 'station_name']][0]

            # check if there is null values
            # print(df.isnull().sum())
            # print(df.isna().sum())

            # check avg tasmin for each month for each station
            # df_1 = df.groupby([df['time'].dt.year, df['time'].dt.month, 'station_name'])['tasmin'].mean()

            # yearly average
            df_1 = df.groupby([df['time'].dt.year, 'station_name'])[df_col].mean()

            # 30 year average
            # df_1 = df.groupby([np.floor(df['time'].dt.year/30)*30, 'station_name'])[df_col].mean()

            # convert series to dataframe
            # df_1 = df_1.to_frame().reset_index()
            df_1 = df_1.reset_index()
            df_1['station_name'] = df_1['station_name'].str.decode('utf-8')

            # pivot dataframe
            df_2 = df_1.pivot('station_name', 'time').stack(0).rename_axis(['station_name', 'value'])

            # convert all int Years into string
            for col in df_2.columns:
                df_2.rename(columns={col:'year_'+ str(col)},inplace=True)

            # finally export
            df_2.to_csv(out_file, index=True, header=True)
            
    