#!/usr/bin/env python
# -*- coding: utf-8 -*- 

__author__ = 'Sunbeam Rahman'
__date__   = '21/03/2023 17:12:04'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ## 

import pandas as pd
import pathlib
from datetime import datetime, timedelta

def process_data(input_dir, output_dir, is_humidity):
    intervals = [(datetime(2021, 1, 1), datetime(2050, 12, 31)),
                 (datetime(2031, 1, 1), datetime(2060, 12, 31)),
                 (datetime(2041, 1, 1), datetime(2070, 12, 31)),
                 (datetime(2051, 1, 1), datetime(2080, 12, 31)),
                 (datetime(2061, 1, 1), datetime(2090, 12, 31)),
                 (datetime(2071, 1, 1), datetime(2100, 12, 31))]

    input_dir = pathlib.Path(input_dir)
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    def adjust_calendar(df, calendar):
        date_col = df.columns[0]
        if calendar == '360_day':
            df[date_col] = pd.to_datetime(df[date_col], format='%Y-%m-%d %H:%M:%S', errors='coerce') - timedelta(days=1)
        return df

    def calculate_yearly_average(df, interval_start, interval_end):
        station_names = df.columns[1:]
        result_df = pd.DataFrame(columns=['station_name', f'year_{interval_start.year}_{interval_end.year}'])

        for station_name in station_names:
            station_df = df[['date', station_name]]            
            interval_df = station_df.loc[station_df['date'].between(interval_start, interval_end)]
            interval_mean = interval_df.mean(numeric_only=True).to_frame().T
            
            interval_mean['station_name'] = station_name
            interval_mean.columns = [f'year_{interval_start.year}_{interval_end.year}', 'station_name']
            result_df = pd.concat([result_df, interval_mean], ignore_index=True)
            print(result_df)
            break
        
        print(result_df)
        return result_df

    def remove_february_nats(df):
        df['month'] = pd.to_datetime(df['date']).dt.month
        df = df[df['month'] != 2]
        df = df.drop(columns='month')
        return df

    for file in input_dir.glob("**/*.csv"):
        out_file = output_dir / (file.stem + ".csv")

        if file.exists() and not out_file.exists():
            df = pd.read_csv(file)
            calendar = '360_day' if is_humidity else '365_day'
            df = adjust_calendar(df, calendar)
            df = remove_february_nats(df)

            interval_dfs = []
            for interval in intervals:
                interval_start, interval_end = interval
                interval_df = calculate_yearly_average(df, interval_start, interval_end)
                interval_dfs.append(interval_df)

            merged_df = pd.concat(interval_dfs, axis=1)
            merged_df_cols = ['station_name'] + [f'year_{interval_start.year}_{interval_end.year}' for interval_start, interval_end in intervals]
            merged_df = merged_df[merged_df_cols]
            print(merged_df)
            # merged_df = merged_df.drop_duplicates(subset='station_name')
            # merged_df.to_csv(out_file, index=False, header=True)

input_dir = r"D:\DOCUMENTS\RIMES\CLIMDATA"
output_dir = r"D:\DOCUMENTS\RIMES\CLIMDATA\output"

process_data(input_dir, output_dir, is_humidity=True)