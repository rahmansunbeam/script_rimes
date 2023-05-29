#!/usr/bin/env python
# -*- coding: utf-8 -*- 

__author__ = 'Sunbeam Rahman'
__date__   = '31/01/2023 12:49:54'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ## 

import arcpy

# This script is for running python inside ArcGIS Pro

aprx = arcpy.mp.ArcGISProject("CURRENT")
map = aprx.listMaps()[0]
tables = [i.name for i in map.listTables()]

# output_fc = r"E:\GIS_Files\GIS_DATA.gdb\climate_cmip6_CanESM5"
output_shp = r"E:\GIS_Files\CMIP6_indices"
# output_gjson = r"D:\Documents"

arcpy.env.qualifiedFieldNames = False

for t in tables:
    joined_fc = arcpy.management.AddJoin('BD_District_clim', 'Dist_nc', t, 'station_name')
    t = t.replace('-', '_')
    t = t.replace('Bangladesh_southasia', '')
    t = t.replace('_districts.csv', '')
    # arcpy.management.CopyFeatures(joined_fc, output_fc + '\BD_District' + t)
    arcpy.management.CopyFeatures(joined_fc, output_shp + '\\' + t)
    arcpy.management.RemoveJoin(joined_fc)
