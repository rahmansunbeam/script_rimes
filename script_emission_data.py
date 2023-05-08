#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Sunbeam Rahman'
__date__   = '09/03/2023 22:00:08'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ##

import urllib.request
import json, csv
import arcpy

# Set the workspace environment
arcpy.env.workspace = "E:\GIS_Files"
arcpy.env.overwriteOutput = True

# output_fishnet = r"E:\GIS_Files\BD_Fishnet.shp"
output_fishnet = r"D:\Data\BD_Emission_all_1.shp"
backup_csv = r"D:\Workspace\script_rimes\backup.csv"

# Construct the API URL for this cell
dim_a_species_param = 'GTS' #CTL, GTS, BFL, CHK, PGS, SHP, ALL
dim_comm_param = 'MILK_PR' #EGGS_PR, MEAT_PR, MILK_PR

count = 0
emissions_dict = {}

# # add field to fishnet shp
# arcpy.management.AddFields(output_fishnet, [["cen_lat", "DOUBLE"], ["cen_lon", "DOUBLE"], ["param", "TEXT"], ["emissions", "DOUBLE"]])

# # Loop through each cell in the fishnet
# with arcpy.da.SearchCursor(output_fishnet, ["OID@", "SHAPE@"]) as search_cur:

#     for search_row in search_cur:

#         extent = search_row[1].extent

#         # project the extent to WGS 1984 (WKID 4326) to get the coordinates in DD
#         # extent_dd = extent.projectAs(arcpy.SpatialReference(4326))
#         extent_dd = extent.projectAs(arcpy.SpatialReference(3857))

#         # Get the extent of the cell
#         xmax, ymax, xmin, ymin = round(extent_dd.XMax, 3), round(extent_dd.YMax, 3), round(extent_dd.XMin, 3), round(extent_dd.YMin, 3)

#         # GLEAM3
#         # url = f"https://io.apps.fao.org/gismgr/api/v1/GLEAM3/EMS/2/wms?request=GetFeatureInfo&tiled=true&query_layers=EMS&dim_src=ALL&bbox={xmin},{ymin},{xmax},{ymax}&format=image/png&dim_gas=CO2EQ&version=1.1.1&transparent=true&exceptions=application/vnd.ogc.se_xml&dim_comm={dim_comm_param}&dim_a_species={dim_a_species_param}&srs=EPSG:4326&service=WMS&layers=EMS&width=256&x=1&feature_count=101&y=195&styles=&info_format=application/json&height=256"

#         # GLW 4: Gridded Livestock Density
#         # url = f"'https://io.apps.fao.org/gismgr/api/v1/GLW4/D_DA/2/wms?request=GetFeatureInfo&tiled=true&query_layers=D_DA&bbox={xmin},{ymin},{xmax},{ymax}&format=image/png&version=1.1.1&transparent=true&exceptions=application/vnd.ogc.se_xml&dim_a_species={dim_a_species_param}&srs=EPSG:3857&service=WMS&layers=D_DA&width=256&x=31&feature_count=101&y=14&styles&info_format=application/json&height=256'"
#         url = f"https://io.apps.fao.org/gismgr/api/v1/GLW4/D_DA/2/wms?request=GetFeatureInfo&tiled=true&query_layers=D_DA&bbox={xmin}%2C{ymin}%2C{xmax}%2C{ymax}&format=image%2Fpng&version=1.1.1&transparent=true&exceptions=application%2Fvnd.ogc.se_xml&dim_a_species={dim_a_species_param}&srs=EPSG%3A3857&service=WMS&layers=D_DA&width=256&x=117&feature_count=101&y=163&styles=&info_format=application%2Fjson&height=256"

#         # Make the API request and parse the JSON response
#         response = urllib.request.urlopen(url)
#         json_data = json.loads(response.read().decode('utf-8'))

#         # Extract the emissions value from the JSON response
#         species = json_data['features'][0]['properties']['Species']
#         # param = json_data['features'][0]['properties']['Commodity']
#         # emissions = json_data['features'][0]['properties']['Emissions (tons of COeq per km²)']
#         density = json_data['features'][0]['properties']['Animal Density (head/km²)']

#         if density not in [None, "", 0.0]:

#             print(json_data)
#             # emissions_dict[search_row[0]] = [param, emissions]
#             emissions_dict[search_row[0]] = [density]
#             # break

# with open(r"D:\Workspace\script_rimes\backup_density_{}_{}.csv".format(dim_a_species_param, dim_comm_param), "w", newline="") as csv_file:
#     writer = csv.writer(csv_file)
#     # write header row
#     writer.writerow(["OID", "density"])
#     # write data rows
#     for oid, values in emissions_dict.items():
#         row = [oid] + values
#         writer.writerow(row)

# with arcpy.da.UpdateCursor(output_fishnet, ["OID@", "param", "emissions"]) as update_cur:

#     for update_row in update_cur:

#          for key, values in emissions_dict.items():

#             try:

#                 if update_row[0] == key:

#                     # update_row[1] = values[0]
#                     update_row[1] = dim_a_species_param
#                     update_row[2] = values[1]
#                     update_cur.updateRow(update_row)
#                     count += 1

#             except StopIteration:
#                 print("No more rows to search.")

# print("Number of rows updated:", count)

oid_param_dict = {}
with open(r"D:\Workspace\script_rimes\backup_density_GTS_MILK_PR.csv", "r") as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        oid = row[0]
        param = row[1]
        oid_param_dict[oid] = param

# loop through features in shp file and update emission field
with arcpy.da.UpdateCursor(output_fishnet, ["OID@", "param", "emissions"]) as cursor:
    for row in cursor:
        if str(row[0]) in oid_param_dict:
            row[1] = dim_a_species_param
            row[2] = oid_param_dict[str(row[0])]
            cursor.updateRow(row)