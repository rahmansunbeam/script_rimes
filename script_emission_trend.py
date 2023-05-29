#!/usr/bin/env python
# -*- coding: utf-8 -*- 

__author__ = 'Sunbeam Rahman'
__date__   = '03/04/2023 20:00:34'
__email__  = 'sunbeam.rahman@live.com'

## -------------------------------- ## 

import arcpy

# Input shapefile
file = "D:\Data\LU_Emission\BD_Emission_all_1.shp"

# Gg/year CO2e
emission_data = {
    '2005': 40495,
    '2015': 41990,
    '2018': 42637,
    '2020': 42767,
    '2030': 44352,
    '2040': 46002,
    '2050': 47718
}  

# Function to check if a field exists
def field_exists(feature_class, field_name):
    field_list = arcpy.ListFields(feature_class, field_name)
    return len(field_list) > 0

# Add fields for normalized densities and emissions
for year in emission_data.keys():
    fields = [
        (f"norm_{year}", "DOUBLE"),
        (f"em_{year}", "DOUBLE"),
        (f"emt_{year}", "DOUBLE"),
    ]
    for field, field_type in fields:
        if not field_exists(file, field):
            arcpy.AddField_management(file, field, field_type)

# Calculate total livestock population
density = []
with arcpy.da.SearchCursor(file, ["density"]) as cursor:
    density = [row[0] for row in cursor if row[0] not in [None, 0]]

total_livestock_pop = sum(density)

# Calculate proportional normalized densities and emissions for each year
for year, national_emis in emission_data.items():
    arcpy.CalculateField_management(
        file, f"norm_{year}",
        "(!density! / {}) if !density! > 0 else 0".format(total_livestock_pop),
        "PYTHON3"
    )

    arcpy.CalculateField_management(
        file, f"em_{year}",
        f"!norm_{year}! * {national_emis}",
        "PYTHON3"
    )

    # 1,000 is used to convert Gg/year CO2e to tons/year CO2e
    # 1,000,000 in the area calculation is used to convert sqm to sqkm
    exp = f"(!em_{year}! * 1000) / (!SHAPE.AREA! / 1000000) if !SHAPE.AREA! > 0 else 0"
    arcpy.CalculateField_management(
        file, f"emt_{year}",
        exp,
        "PYTHON3"
    )

    print("{} added".format(str(year)))

