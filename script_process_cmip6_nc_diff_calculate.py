#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Sunbeam Rahman'
__date__   = '22/08/2023 14:47:30'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ##

from pathlib import Path
from itertools import zip_longest
from datetime import datetime
import pandas as pd
import json

def process_historical_projected_diff_json(files_historical, files_projected, json_output):

    files_historical = [file for file in Path(files_historical).glob("**\*.json")]
    files_projected = [file for file in Path(files_projected).glob("**\*.json")]

    for file_historical, file_projected in zip_longest(files_historical, files_projected):

        result_dict = {}

        with file_historical.open() as f:
            json_hist = json.load(f)

        with file_projected.open() as f:
            json_proj = json.load(f)

        for key_hist, value_hist in json_hist.items():

            for key_proj, value_proj in json_proj.items():

                if 'station' in (key_hist, key_proj):
                    continue

                key_hist_month = datetime.strptime(key_hist.split('_')[-1], '%b')
                key_proj_month = datetime.strptime(key_proj.split('_')[-1], '%b')

                if key_hist_month == key_proj_month:
                    diff_values = [value_hist[month] - value_proj[month] for month in value_proj]
                    result_dict[key_proj] = {str(i): diff for i, diff in enumerate(diff_values)}

        json_output = Path(json_output)
        json_output.mkdir(parents=True, exist_ok=True)

        output_file_name = json_output / (file_projected.stem + "_hist_to_projected_diff.json")
        with output_file_name.open('w') as f:
            json.dump(result_dict, f, indent=4)

def process_historical_with_interval_projected_diff_json(files_historical, files_projected, json_output):

    intervals = [(pd.Timestamp('1950-01-01'), pd.Timestamp('1979-12-31')),
                (pd.Timestamp('1955-01-01'), pd.Timestamp('1984-12-31'))]
                # (pd.Timestamp('1960-01-01'), pd.Timestamp('1989-12-31')),
                # (pd.Timestamp('1965-01-01'), pd.Timestamp('1994-12-31')),
                # (pd.Timestamp('1970-01-01'), pd.Timestamp('1999-12-31')),
                # (pd.Timestamp('1975-01-01'), pd.Timestamp('2004-12-31')),
                # (pd.Timestamp('1980-01-01'), pd.Timestamp('2009-12-31')),
                # (pd.Timestamp('1985-01-01'), pd.Timestamp('2014-12-31'))]

    files_historical = [file for file in Path(files_historical).glob("**\*.json") if 'div' not in file.name]
    files_projected = [file for file in Path(files_projected).glob("**\*.json") if 'div' not in file.name]

    for file_historical, file_projected in zip_longest(files_historical, files_projected):

        result_dict = {}

        for interval in intervals:
            interval_start, interval_end = interval

            with file_historical.open() as f:
                json_hist = json.load(f)

            with file_projected.open() as f:
                json_proj = json.load(f)

            for key_hist, value_hist in json_hist.items():

                key_hist_year_start = datetime.strptime(key_hist.split('_')[2], '%Y')
                key_hist_year_end = datetime.strptime(key_hist.split('_')[3], '%Y')
                key_hist_month = datetime.strptime(key_hist.split('_')[1], '%b')

                for key_proj, value_proj in json_proj.items():

                    if 'station' in (key_hist, key_proj):
                        continue

                    key_proj_month = datetime.strptime(key_proj.split('_')[-1], '%b')
                    key_proj_year = datetime.strptime(key_proj.split('_')[-2], '%Y')

                    if interval_start.year <= key_hist_year_start.year <= interval_end.year and interval_start.year <= key_hist_year_end.year <= interval_end.year:

                        if key_hist_month == key_proj_month:

                            interval_key = f"diff_{interval_start.year}-{interval_end.year}_{key_proj_year.year}_{key_proj_month.strftime('%b')}"
                            diff_values = [value_hist[month] - value_proj[month] for month in value_proj]
                            result_dict[interval_key] = {str(i): diff for i, diff in enumerate(diff_values)}

        json_output = Path(json_output)
        json_output.mkdir(parents=True, exist_ok=True)

        output_file_name = json_output / (file_projected.stem + "_hist_to_projected_diff.json")
        with output_file_name.open('w') as f:
            json.dump(result_dict, f, indent=4)

json_historical_mon = r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\historical\r1i1p1f1\ACCESS-CM2_hist_monthly_sum_interval"
json_proj = r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\historical\r1i1p1f1\ACCESS-CM2_ssp245_sum\ACCESS-CM2_monthly"
json_output = r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\historical\r1i1p1f1\output"

# process_historical_projected_diff_json(json_historical_mon, json_proj, json_output)
process_historical_with_interval_projected_diff_json(json_historical_mon, json_proj, json_output)

