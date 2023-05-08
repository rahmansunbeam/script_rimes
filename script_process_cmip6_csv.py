#!/usr/bin/env python
# -*- coding: utf-8 -*- 

__author__ = 'Sunbeam Rahman'
__date__   = '02/05/2023 11:27:23'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ## 

import pandas as pd
import re
import pathlib

input_dir = pathlib.Path(r"D:\Data\Bangladesh_indices\ACCESS-CM2")
output_dir = input_dir / "output"
output_dir.mkdir(parents=True, exist_ok=True)

for file in input_dir.glob("**\*.txt"):

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
        # csv.to_json(out_file)
        
        break