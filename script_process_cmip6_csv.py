#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Sunbeam Rahman'
__date__   = '02/05/2023 11:27:23'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ##

import pandas as pd
import re
import pathlib

input_dir = pathlib.Path(r"D:\Data\CLIMDATA_MAIN\UKESM1-0-LL")
output_dir = input_dir / "output"
output_dir.mkdir(parents=True, exist_ok=True)

def process_def_1(file):
    f_name = file.stem.replace('index_', '')
    f_name = f_name.replace('_districts', '')
    f_name = f_name.replace('_divisions', '_div')
    out_file = output_dir / (f_name + ".json")

    if re.search("cwd", file.stem):

        csv = pd.read_csv(file, index_col=False).fillna(0)
        csv.set_index(['Date'], inplace=True)

        csv_transpose = csv.T

        csv = csv_transpose.rename(columns={"Date": "Station"})

        print(file.stem)
        print(csv)
        csv.to_json(out_file)
        # break

def process_def_2(file):

    f_name = file.stem.replace('_southasia', '')
    f_name = f_name.replace('_districts', '')
    f_name = f_name.replace('_provinces', '_prv')
    out_file = output_dir / (f_name + ".json")

    csv = pd.read_csv(file, index_col=False).fillna(0)
    csv['date'] = pd.to_datetime(csv['date'], format='%Y-%m-%d %H:%M:%S')
    csv_2 = csv.groupby(csv["date"].dt.year).mean().rename(columns = {"date":"Station_name"})

    csv_3 = csv_2.T

    print(file.stem)
    csv_3.to_json(out_file)

def process_def_3(file):
    out_file = output_dir / (file.stem + ".csv")
    csv = pd.read_csv(file, index_col=False).fillna(0)
    csv['date'] = "year_" + pd.to_datetime(csv['date']).dt.year.astype(str)

    csv_2 = csv.T
    csv_2.columns = csv_2.iloc[0]
    csv_2 = csv_2.iloc[1:]
    
    print(file.stem)
    csv_2.to_csv(out_file)

def process_def_4(file):
    out_file = output_dir / (file.stem + ".csv")
    csv = pd.read_csv(file, index_col=False).fillna(0)
    
    if 'monthly' in file.stem:
        csv['date'] = "month_" + pd.to_datetime(csv['date'], errors='coerce').dt.to_period('M').astype(str)
    elif 'seasonal' in file.stem:
        seasons = {1: 'DJF', 2: 'DJF', 3: 'MAM', 4: 'MAM', 5: 'MAM', 6: 'JJA', 7: 'JJA', 8: 'JJA', 9: 'SON', 10: 'SON', 11: 'SON', 12: 'DJF'}
        date_series = pd.to_datetime(csv['date'], errors='coerce')
        csv['date'] = "season_" + date_series.dt.year.astype(str) + '_' + date_series.dt.month.map(seasons)
    else:
        csv['date'] = "year_" + pd.to_datetime(csv['date'], errors='coerce').dt.year.astype(str)

    csv_2 = csv.T
    csv_2.columns = csv_2.iloc[0]
    csv_2 = csv_2.iloc[1:]
    
    print(file.stem)
    csv_2.to_csv(out_file)

for file in input_dir.glob("*.csv"):
    process_def_4(file)
