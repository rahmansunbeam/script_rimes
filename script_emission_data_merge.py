#!/usr/bin/env python
# -*- coding: utf-8 -*- 

__author__ = 'Sunbeam Rahman'
__date__   = '24/03/2023 13:22:13'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ## 

import arcpy
from pathlib import Path

# Set the workspace to a folder containing your shapefiles
arcpy.env.workspace = 'E:\GIS_Files\GIS_DATA.gdb\emission_data'
arcpy.env.overwriteOutput = True

input_fcs = [Path(fc) for fc in arcpy.ListFeatureClasses(wild_card='*emission*')]
output_fc = Path('E:\GIS_Files\GIS_DATA.gdb\emission_data\BD_All')
districts_fc = 'E:\GIS_Files\GIS_DATA.gdb\Admin_Boundary\BD_District_emission'

if not arcpy.Exists(str(output_fc)):
    sr = arcpy.Describe(str(input_fcs[0])).spatialReference
    arcpy.CreateFeatureclass_management(str(output_fc.parent), str(output_fc.name), 'POLYGON', spatial_reference=sr)

# # Loop through each input shapefile and merge the features into the output feature class
# for fc in input_fcs:
#     layer_name = str(fc.stem)
#     arcpy.MakeFeatureLayer_management(str(fc), layer_name)

#     # Add a new field to the output shapefile with the layer name as a prefix
#     new_field_name = f"{layer_name[-8:]}_em"
#     arcpy.AddField_management(str(output_fc), new_field_name, "DOUBLE")

#     with arcpy.da.SearchCursor(layer_name, ["OID@", "emissions"]) as in_cursor:
#         for in_row in in_cursor:
#             in_oid, in_emissions = in_row
#             with arcpy.da.UpdateCursor(str(output_fc), ["OID@", new_field_name]) as out_cursor:
#                 for out_row in out_cursor:
#                     out_oid = out_row[0]
#                     if in_oid == out_oid:
#                         out_row[1] = in_emissions
#                         out_cursor.updateRow(out_row)
#                         break # to avoid looping over and over

# arcpy.Delete_management("in_memory")

# Calculate the mean of each emission field based on the districts
for field in arcpy.ListFields(str(output_fc), wild_card='*em', field_type='Double'):

    print("processing - {}".format(field.name))

    try:
        out_feature_class = "E:\GIS_Files\GIS_DATA.gdb\emission_data\sp_{}".format(field.name)
        arcpy.SpatialJoin_analysis(districts_fc, str(output_fc), out_feature_class, match_option='HAVE_THEIR_CENTER_IN')
        arcpy.JoinField_management(districts_fc, "OBJECTID", out_feature_class, "TARGET_FID", [field.name])

    finally:
        arcpy.Delete_management(out_feature_class)
