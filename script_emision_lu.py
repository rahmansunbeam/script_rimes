#!/usr/bin/env python
# -*- coding: utf-8 -*- 

__author__ = 'Sunbeam Rahman'
__date__   = '30/03/2023 12:54:11'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ## 

import pathlib
import xarray as xr

input_dir = pathlib.Path(r"D:\Data\LU_Emission")

for file in input_dir.glob("**\*.nc"):
    
    output_dir = input_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    out_file = output_dir / (file.stem + ".csv")

    ds = xr.open_dataset(file)
    df = ds.to_dataframe()
    df = df.reset_index()

    df_2 = df[(df['lon'] > 0) & (df['lat'] > 0)]

    df_2.to_csv(out_file, index=True, header=True)
