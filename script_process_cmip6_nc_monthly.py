#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Sunbeam Rahman'
__date__   = '08/06/2023 16:11:45'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ##

import pathlib
import xarray as xr
import pandas as pd
from datetime import datetime

input_dir = pathlib.Path(r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\ssp245\r1i1p1f1\tas")

# Define month mapping
month_dict = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

for file in input_dir.glob("*.nc"):

    output_dir = input_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    f_name = file.stem.replace('index_', '').replace('Bangladesh_southasia', 'BD').replace('_districts', '').replace('_divisions', '_div')
    out_file = output_dir / (f_name + "_monthly.csv")

    if file.exists() and not out_file.exists():

        print("processing: {}".format(file))

        ds = xr.open_dataset(file)
        df = ds.to_dataframe().reset_index()
        df_col = [col for col in df.columns if col not in ['time', 'station', 'station_name']][0]
        df_1 = df.groupby([df['time'].dt.year.astype(str) + "_" + df['time'].dt.month.map(month_dict), 'station_name'])[df_col].mean()

        df_1.index = df_1.index.set_levels(df_1.index.levels[0].map(lambda x: datetime.strptime(x, '%Y_%b')), level=0)
        df_1.sort_index(inplace=True)

        df_2 = df_1.reset_index().pivot('station_name', 'time').stack(0).rename_axis(['station_name', 'value'])

        for col in df_2.columns:
            form_col = pd.to_datetime(col).strftime('%Y_%b')
            df_2.rename(columns={col:'month_'+ form_col},inplace=True)

        df_2.to_csv(out_file, index=True, header=True)
