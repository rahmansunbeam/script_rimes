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
        result_df = {}

        for station_name in station_names:
            station_df = df[['date', station_name]]
            interval_df = station_df.loc[station_df['date'].between(interval_start, interval_end)]
            interval_mean = interval_df.mean(numeric_only=True)
            result_df[interval_mean.keys()[0]] = interval_mean[station_name]

        return result_df

    def remove_february_nats(df):
        df['month'] = pd.to_datetime(df['date']).dt.month
        df = df[df['month'] != 2]
        df = df.drop(columns='month')
        return df

    for file in input_dir.glob("**/*daily_ssp245.csv"):
        out_file = output_dir / (file.stem + ".csv")

        if file.exists() and not out_file.exists():
            df = pd.read_csv(file)
            calendar = '360_day' if is_humidity else '365_day'
            df = adjust_calendar(df, calendar)
            df = remove_february_nats(df)

            output_dfs = {}

            for interval in intervals:
                interval_start, interval_end = interval
                interval_df = calculate_yearly_average(df, interval_start, interval_end)

                for key, value in interval_df.items():
                    output_dfs[key] = output_dfs.get(key, []) + [value]

            output_dfs = pd.DataFrame(output_dfs).T.reset_index()
            output_dfs.columns = ['station_name'] + [f'year_{interval_start.year}_{interval_end.year}' for interval_start, interval_end in intervals]
            output_dfs.to_csv(out_file, index=False, header=True)

input_dir = r"D:\Data\CLIMDATA_MAIN\UKESM1-0-LL\output"
output_dir = r"D:\Data\CLIMDATA_MAIN\UKESM1-0-LL\output\output"

process_data(input_dir, output_dir, is_humidity=True)
