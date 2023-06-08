#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Sunbeam Rahman'
__date__   = '08/06/2023 16:39:31'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ##

import pathlib
import xarray as xr
import pandas as pd
from datetime import datetime

input_dir = pathlib.Path(r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\ssp245\r1i1p1f1\tas")

seasons = {'DJF': [12, 1, 2], 'MAM': [3, 4, 5], 'JJA': [6, 7, 8], 'SON': [9, 10, 11]}

for file in input_dir.glob("*.nc"):

    output_dir = input_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    f_name = file.stem.replace('index_', '').replace('Bangladesh_southasia', 'BD').replace('_districts', '').replace('_divisions', '_div')

    out_file = output_dir / (f_name + "_seas.csv")

    if file.exists() and not out_file.exists():

        print("processing: {}".format(file))

        ds = xr.open_dataset(file)
        df = ds.to_dataframe().reset_index()

        # Map each month to its season
        df['season'] = df['time'].dt.month.apply(lambda month: next(season for season, months in seasons.items() if month in months))

        # Create a 'year_season' column
        df['year_season'] = df['time'].dt.year.astype(str) + "_" + df['season']

        df_col = [col for col in df.columns if col not in ['time', 'station', 'station_name', 'season', 'year_season']][0]

        # Group by 'year_season' and station_name to calculate the mean
        df_1 = df.groupby(['year_season', 'station_name'])[df_col].mean().reset_index()

        df_2 = df_1.pivot('station_name', 'year_season').stack(0).rename_axis(['station_name', 'value'])

        # Reorder the columns by converting year_season to a categorical type with a specified order
        year_seas_cat = pd.Categorical(df_1['year_season'], categories=[f"{year}_{season}" for year in df_1['year_season'].str[:4].unique() for season in list(seasons.keys())], ordered=True)
        df_2.columns = df_2.columns.astype(year_seas_cat.dtype)
        df_2 = df_2.sort_index(axis=1)

        df_2.columns = ['seas_' + str(col) for col in df_2.columns]
        df_2.to_csv(out_file, index=True, header=True)
