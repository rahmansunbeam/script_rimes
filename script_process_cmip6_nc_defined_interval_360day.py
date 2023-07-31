#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Sunbeam Rahman'
__date__   = '21/03/2023 17:12:04'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ##

import pandas as pd
import pathlib
from datetime import datetime, timedelta

def process_data(input_dir, output_dir, is_humidity, average_typ):

    intervals = [(datetime(2021, 1, 1), datetime(2050, 12, 31)),
                 (datetime(2031, 1, 1), datetime(2060, 12, 31)),
                 (datetime(2041, 1, 1), datetime(2070, 12, 31)),
                 (datetime(2051, 1, 1), datetime(2080, 12, 31)),
                 (datetime(2061, 1, 1), datetime(2090, 12, 31)),
                 (datetime(2071, 1, 1), datetime(2100, 12, 31))]

    months = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

    seasons = [('DJF', [12, 1, 2]), ('MAM', [3, 4, 5]), ('JJA', [6, 7, 8]), ('SON', [9, 10, 11])]

    input_dir = pathlib.Path(input_dir)
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    def adjust_calendar(df, calendar):

        date_col = df.columns[0]

        if calendar == '360_day':
            df[date_col] = pd.to_datetime(df[date_col], format='%Y-%m-%d %H:%M:%S', errors='coerce') - timedelta(days=1)
            df = df.fillna(method='ffill').fillna(method='bfill')

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

    def calculate_monthly_average(df, interval_start, interval_end):

        station_names = df.columns[1:]
        result_df = {}

        for station_name in station_names:
            station_df = df[['date', station_name]]
            interval_mask = station_df['date'].between(interval_start, interval_end)
            averages = []

            for month, month_name in months.items():
                monthly_df = station_df[interval_mask & (station_df['date'].dt.month == month)]

                if not monthly_df.empty:
                    interval_mean = monthly_df.mean(numeric_only=True)
                    averages.append(interval_mean[station_name])

            result_df[f'{station_name}'] = averages
            # break

        return result_df

    def calculate_seasonal_average(df, interval_start, interval_end):

        station_names = df.columns[1:]
        result_df = {}

        for station_name in station_names:
            station_df = df[['date', station_name]]
            interval_mask = station_df['date'].between(interval_start, interval_end)
            averages = []

            for season, months in seasons:
                seasonal_df = station_df[interval_mask & (station_df['date'].dt.month.isin(months))]

                if not seasonal_df.empty:
                    interval_mean = seasonal_df.mean(numeric_only=True)
                    averages.append(interval_mean[station_name])

            result_df[f'{station_name}'] = averages
            # break

        return result_df

    for file in input_dir.glob("*daily_ssp245.csv"):

        out_file = output_dir / (file.stem + "_" + average_typ + ".csv")

        if file.exists() and not out_file.exists():

            print("processing: {}".format(file))

            df = pd.read_csv(file)
            calendar = '360_day' if is_humidity else '365_day'
            df = adjust_calendar(df, calendar)

            output_dfs = {}

            for interval in intervals:
                interval_start, interval_end = interval

                if average_typ == 'yearly':

                    interval_df = calculate_yearly_average(df, interval_start, interval_end)

                    for key, value in interval_df.items():
                        output_dfs[key] = output_dfs.get(key, []) + [value]

                elif average_typ == 'monthly':

                    interval_df = calculate_monthly_average(df, interval_start, interval_end)

                    for key, value in interval_df.items():
                        output_dfs[key] = output_dfs.get(key, []) + [value][0]

                elif average_typ == 'seasonal':

                    interval_df = calculate_seasonal_average(df, interval_start, interval_end)

                    for key, value in interval_df.items():
                        output_dfs[key] = output_dfs.get(key, []) + [value][0]

                else:
                    print('average_type is incorrect')

            if average_typ == 'yearly':
                output_dfs = pd.DataFrame(output_dfs).T.reset_index()
                output_dfs.columns = ['station_name'] + [f'year_{interval_start.year}_{interval_end.year}' for interval_start, interval_end in intervals]
                output_dfs.to_csv(out_file, index=False, header=True)

            elif average_typ == 'monthly':
                output_dfs = pd.DataFrame.from_dict(output_dfs, orient='index').reset_index()
                output_dfs.columns = ['station_name'] + [f'mon_{interval_start.year}_{interval_end.year}_{month_name}' for
                                                                interval_start, interval_end in intervals for month_num, month_name in months.items()]

                output_dfs.to_csv(out_file, index=False, header=True)

            elif average_typ == 'seasonal':
                output_dfs = pd.DataFrame.from_dict(output_dfs, orient='index').reset_index()
                output_dfs.columns = ['station_name'] + [f'seas_{interval_start.year}_{interval_end.year}_{season}' for
                                                                interval_start, interval_end in intervals for season, months in seasons]
                output_dfs.to_csv(out_file, index=False, header=True)

            else:
                print('average_type is incorrect')
        
        else:
            print('output file already exists or input doesn\'t exists')

input_dir = r"D:\Data\CLIMDATA_MAIN\UKESM1-0-LL\output"
output_dir = r"D:\Data\CLIMDATA_MAIN\UKESM1-0-LL\output\output"

process_data(input_dir, output_dir, is_humidity=True, average_typ='yearly') # yearly, monthly, seasonal
