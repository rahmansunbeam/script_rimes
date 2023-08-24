#!/usr/bin/env python
# -*- coding: utf-8 -*- 

__author__ = 'Sunbeam Rahman'
__date__   = '22/08/2023 14:47:30'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ## 

from pathlib import Path
from itertools import zip_longest
from datetime import datetime
import json

def process_historical_current_diff_json(files_historical, files_current, json_output):

    files_historical = [file for file in Path(files_historical).glob("**\*.json")]
    files_current = [file for file in Path(files_current).glob("**\*.json")]

    for file_historical, file_current in zip_longest(files_historical, files_current):

        result_dict = {}

        with file_historical.open() as f:
            json_hist = json.load(f)

        with file_current.open() as f:
            json_curr = json.load(f)

        for key_hist, value_hist in json_hist.items():

            for key_curr, value_curr in json_curr.items():

                if 'station' in (key_hist, key_curr):
                    continue

                key_hist_month = datetime.strptime(key_hist.split('_')[-1], '%b')
                key_curr_month = datetime.strptime(key_curr.split('_')[-1], '%b')
                
                if key_hist_month == key_curr_month:
                    diff_values = [value_hist[month] - value_curr[month] for month in value_curr]
                    result_dict[key_curr] = {str(i): diff for i, diff in enumerate(diff_values)}

        json_output = Path(json_output)
        json_output.mkdir(parents=True, exist_ok=True)

        output_file_name = json_output / (file_current.stem + "_hist_to_projected_diff.json")
        with output_file_name.open('w') as f:
            json.dump(result_dict, f, indent=4)

json_historical_mon = r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\historical\r1i1p1f1\ACCESS-CM2_hist_monthly_sum"
json_curr = r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\historical\r1i1p1f1\ACCESS-CM2_ssp245_avg\ACCESS-CM2_monthly"
json_output = r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\historical\r1i1p1f1\output"

process_historical_current_diff_json(json_historical_mon, json_curr, json_output)
