#!/usr/bin/env python
# -*- coding: utf-8 -*- 

__author__ = 'Sunbeam Rahman'
__date__   = '22/05/2023 12:24:16'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ## 

import urllib.request
from pathlib import Path

# https://ds.nccs.nasa.gov/thredds/ncss/AMES/NEX/GDDP-CMIP6/NorESM2-MM/ssp245/r1i1p1f1/hurs/hurs_day_NorESM2-MM_ssp245_r1i1p1f1_gn_2015.nc?var=hurs&north=26.7&west=88.04&east=92.68&south=20.64&horizStride=1&time_start=2015-01-01T12%3A00%3A00Z&time_end=2015-12-31T12%3A00%3A00Z&timeStride=1

base_url = "https://ds.nccs.nasa.gov/thredds/ncss/AMES/NEX/GDDP-CMIP6/NorESM2-MM/ssp245/r1i1p1f1/hurs/"

north = 26.70
south = 20.64
west = 88.04
east = 92.68

start_year = 2015
end_year = 2100

download_dir = Path(r"D:\Data\CLIMDATA_MAIN\NorESM2-MM\ssp245")
download_dir.mkdir(parents=True, exist_ok=True)

def cmip6_download_data(download_dir, base_url):

    for year in range(start_year, end_year + 1):
        file_url = base_url + f"hurs_day_NorESM2-MM_ssp245_r1i1p1f1_gn_{year}.nc?var=hurs&north={north}&west={west}&east={east}&south={south}&horizStride=1&time_start={year}-01-01T12%3A00%3A00Z&time_end={year}-12-31T12%3A00%3A00Z&timeStride=1"
        file_name = f"BD_hurs_day_NorESM2-MM_ssp245_r1i1p1f1_{year}.nc"
        file_path = download_dir / file_name

        if not file_path.is_file() :
            print(f"Downloading {file_name}...")
            urllib.request.urlretrieve(file_url, file_path)

    print("Download complete!")

cmip6_download_data(download_dir, base_url)

# yearly mean nc file from daily data
# cdo -yearmean -cat 'BD_hurs*' yearlyMeanOutput.nc

# PyScissor process yearly average with combines nv file with shp
# nc2ts_by_shp.py -nc=yearlyMeanOutput.nc -nci='Y=lat;X=lon;T=time;V=hurs;' -sf=/mnt/d/Data/Administrative_Bnd/BD_District.shp -sfp='Dist_rimes' -r=wavg -o=test3.csv