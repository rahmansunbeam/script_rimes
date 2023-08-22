#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Sunbeam Rahman'
__date__   = '11/01/2023 22:09:21'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ##

import pathlib
import xarray as xr
import pandas as pd
from datetime import datetime

def process_data(input_dir, output_dir, average_typ, accumulated):

    month_dict = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
    seasons = {'DJF': [12, 1, 2], 'MAM': [3, 4, 5], 'JJA': [6, 7, 8], 'SON': [9, 10, 11]}

    input_dir = pathlib.Path(input_dir)
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # if average_typ in ["monthly", "seasonal"]:
    #     input_dir_query = "**\*month*.nc"
    # else:
    input_dir_query = "**\*.nc"

    for file in input_dir.glob(input_dir_query):

        f_name = file.stem.replace('southasia_', '').replace('Bangladesh_', '').replace('index_', '').replace('_districts', '').replace('_divisions', '_div')

        out_file = output_dir / (f_name + ".json")

        if file.exists():

            if not out_file.exists():

                print("processing: {}".format(file))

                ds = xr.open_dataset(file)
                df = ds.to_dataframe().reset_index()
                df_col = [col for col in df.columns if col not in ['time', 'bnds', 'station', 'time_bnds', 'station_name']][0]

                if average_typ == 'yearly':
                    
                    if accumulated == 'sum':
                        if file.parent.name == "pr":
                            df_1 = df.groupby([df['time'].dt.year, 'station'])[df_col].sum().reset_index()
                        else:
                            df_1 = df.groupby([df['time'].dt.year, 'station'])[df_col].mean().reset_index()
                    elif accumulated == 'avg':
                        df_1 = df.groupby([df['time'].dt.year, 'station'])[df_col].mean().reset_index()
                    else:
                        print('error in accumulated param')

                    df_2 = df_1.pivot('station', 'time').stack(0).rename_axis(['station', 'value']).reset_index().drop(['value'], axis=1)

                    for col in df_2.columns:
                        df_2.rename(columns={col:'year_'+ str(col)},inplace=True)

                elif average_typ == 'monthly':

                    if accumulated == 'sum':
                        if file.parent.name == "pr":
                            # df_1 = df.groupby([df['time'].dt.year.astype(str) + "_" + df['time'].dt.month.map(month_dict), 'station'])[df_col].sum()
                            df_1 = df.groupby([df['time'].dt.month.map(month_dict), 'station'])[df_col].sum()/64
                        else:
                            # df_1 = df.groupby([df['time'].dt.year.astype(str) + "_" + df['time'].dt.month.map(month_dict), 'station'])[df_col].mean()
                            df_1 = df.groupby([df['time'].dt.month.map(month_dict), 'station'])[df_col].mean()
                    elif accumulated == 'avg':
                        df_1 = df.groupby([df['time'].dt.year.astype(str) + "_" + df['time'].dt.month.map(month_dict), 'station'])[df_col].mean()
                    else:
                        print('error in accumulated param')

                    # df_1.index = df_1.index.set_levels(df_1.index.levels[0].map(lambda x: datetime.strptime(x, '%Y_%b')), level=0)
                    df_1.index = df_1.index.set_levels(df_1.index.levels[0].map(lambda x: datetime.strptime(x, '%b')), level=0)
                    df_1.sort_index(inplace=True)
                    df_2 = df_1.reset_index().pivot('station', 'time').stack(0).rename_axis(['station', 'value']).reset_index().drop(['value'], axis=1)

                    for col in df_2.columns:
                        if col not in ['station', 'time']:
                            # form_col = pd.to_datetime(col).strftime('%Y_%b')
                            form_col = pd.to_datetime(col).strftime('%b')
                            df_2.rename(columns={col:'month_'+ form_col},inplace=True)

                elif average_typ == 'seasonal':

                    df['season'] = df['time'].dt.month.apply(lambda month: next(season for season, months in seasons.items() if month in months))
                    df['year_season'] = df['time'].dt.year.astype(str) + "_" + df['season']

                    if accumulated == 'sum':
                        if file.parent.name == "pr":
                            df_1 = df.groupby(['year_season', 'station'])[df_col].sum().reset_index()
                        else:
                            df_1 = df.groupby(['year_season', 'station'])[df_col].mean().reset_index()
                    elif accumulated == 'avg':
                        df_1 = df.groupby(['year_season', 'station'])[df_col].mean().reset_index()
                    else:
                        print('error in accumulated param')

                    df_2 = df_1.pivot('station', 'year_season').stack(0).rename_axis(['station', 'value']).reset_index().drop(['value'], axis=1)

                    year_seas_cat = pd.Categorical(df_1['year_season'], 
                                    categories=[f"{year}_{season}" for year in df_1['year_season'].str[:4].unique() for season in list(seasons.keys())], 
                                    ordered=True)
                    df_2.columns = df_2.columns.astype(year_seas_cat.dtype)
                    df_2 = df_2.sort_index(axis=1)
                    df_2.columns = ['seas_' + str(col) for col in df_2.columns]

                else:
                    print("Problem !!")

                # finally export
                df_2.to_json(out_file)

input_dir = r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\historical\r1i1p1f1"
output_dir = r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\historical\r1i1p1f1\output"

process_data(input_dir, output_dir, average_typ='monthly', accumulated = 'sum') # yearly, monthly, seasonal; avg, sum
