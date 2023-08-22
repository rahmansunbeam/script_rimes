#!/usr/bin/env python
# -*- coding: utf-8 -*- 

__author__ = 'Sunbeam Rahman'
__date__   = '22/08/2023 14:47:30'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ## 

from pathlib import Path
from datetime import datetime
import pandas as pd
import json

json_historical_mon = r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\historical\r1i1p1f1\ACCESS-CM2_hist_monthly_sum"
json_current = r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\historical\r1i1p1f1\ACCESS-CM2_ssp245_avg\ACCESS-CM2_monthly"
json_output = Path(r"D:\Data\Bangladesh_CMIP6_sublevels\ACCESS-CM2\historical\r1i1p1f1\output")

files_base = [file for file in Path(json_historical_mon).glob("**\*.json") if 'div' not in file.name]
files_current = [file for file in Path(json_current).glob("**\*.json") if 'div' not in file.name]

for file_base in files_base:
    for file_current in files_current:

        result_dict = {}

        with file_base.open() as f:
            json_base = eval(f.read())

        with file_current.open() as f:
            json_current = eval(f.read())

        if 'pr' in file_base.name and 'pr' in file_current.name:

            diff_dict = {}
            # key_list = [str(i) for i in range(64)]

            for key_base, value_base in json_base.items():
                for key_current, value_current in json_current.items():
                    if key_base != 'station' and key_current != 'station':
                        key_base_month = datetime.strptime(key_base.split('_')[-1], '%b')
                        key_curr_year = datetime.strptime(key_current.split('_')[1], '%Y')
                        key_curr_month = datetime.strptime(key_current.split('_')[-1], '%b')
                        
                        if key_base_month == key_curr_month:
                            diff_values = [value_current[month] - value_base[month] for month in value_current]   
                            d1 = dict()
                            for i in range(len(diff_values)):
                                d1[str(i)] = diff_values[i]                     
                            result_dict[key_current] = d1                            
                            print(result_dict)
                            break

            json_output.mkdir(parents=True, exist_ok=True)

            output_file_name = json_output / (file_current.stem + "_diff.json")
            with output_file_name.open('w') as f:
                f.write(json.dumps(result_dict, indent=4))

