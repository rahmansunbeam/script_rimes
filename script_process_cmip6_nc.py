#!/usr/bin/env python
# -*- coding: utf-8 -*- 

__author__ = 'Sunbeam Rahman'
__date__   = '11/01/2023 22:09:21'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ## 

import pathlib
import xarray as xr
import numpy as np
from datetime import datetime

input_dir = pathlib.Path(r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\ssp245\r1i1p1f1\tas")

for file in input_dir.glob("*districts.nc"):
    
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
            month_dict = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
            df_1 = df.groupby([df['time'].dt.year.astype(str) + "_" + df['time'].dt.month.map(month_dict), 'station_name'])[df_col].mean()

            def parse_year_month(year_month_str):
                return datetime.strptime(year_month_str, '%Y_%b')
            
            df_1.index.set_levels(df_1.index.levels[0].map(parse_year_month), level=0, inplace=True)

            df_1.sort_index(inplace=True)

            # yearly average
            # df_1 = df.groupby([df['time'].dt.year, 'station_name'])[df_col].mean()

            # 30 year average
            # df_1 = df.groupby([np.floor(df['time'].dt.year/30)*30, 'station_name'])[df_col].mean()

            # convert series to dataframe
            # df_1 = df_1.to_frame().reset_index()
            df_1 = df_1.reset_index()
            # df_1['station_name'] = df_1['station_name'].str.decode('utf-8')

            # pivot dataframe
            df_2 = df_1.pivot('station_name', 'time').stack(0).rename_axis(['station_name', 'value'])

            # convert all int Years into string
            for col in df_2.columns:
                # df_2.rename(columns={col:'year_'+ str(col)},inplace=True)
                df_2.rename(columns={col:'month_'+ str(col)},inplace=True)

            # finally export
            df_2.to_csv(out_file, index=True, header=True)
            # df_2.to_json(out_file)
            
    