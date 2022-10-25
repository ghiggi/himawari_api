#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 14:29:20 2022

@author: ghiggi
"""

##----------------------------------------------------------------------------.
###4 Test patterns 
from himawari_api.listing import GLOB_FNAME_PATTERN
from trollsift import Parser

fname = "HS_H08_20210606_0050_B01_FLDK_R10_S0110.DAT.bz2"
fname = "HS_H08_20200202_0010_B01_R302_R10_S0101.DAT.bz2"
sensor = "AHI"
product_level = "L1b"
   
# Retrieve file pattern
fpattern = GLOB_FNAME_PATTERN[sensor][product_level]

fpattern = 'HS_{platform_shortname:3s}_{start_time:%Y%m%d_%H%M}_B{band_number:2d}_{sector:4s}_R{spatial_res:2d}_S{segment_number:2d}{segment_total:2d}.{data_format}'
fpattern = 'HS_{platform_shortname:3s}_{start_time:%Y%m%d_%H%M}_B{band_number:2d}_{sector:4s}_R{spatial_res:2d}_S{segment_number:2d}{segment_total:2d}.{data_format}'

# Retrieve information from filename 
p = Parser(fpattern)
info_dict = p.parse(fname)
info_dict
   
   
##----------------------------------------------------------------------------.
# M1 , M2

 
# region or scene_abbr

# satellite = "HIMAWARI-8"
# sector = [FLDK, Japan, Target]
# product = ["Rad", "CMSK", "CHGT", "CPHS", "RRQPE"]
# region = "01","02",03", "04"       # FOR JAPAN  
# region = "301", "302", "303", 304" # For Target 



sector = info_dict['sector']
if sector not in "FLDK": 
    if "JP" in sector: 
        region = sector.replace("JP","")
        sector = "Japan"
        info_dict['sector'] = sector 
        info_dict['region'] = region 
    elif "R" in sector: 
        region = sector.replace("R","")
        sector = "Target"
        info_dict['sector'] = sector 
        info_dict['region'] = region 
        
        
        
get_bucket(protocol="s3", satellite="himawari-8")

sensor = "AHI"
product_level = "L1b"
product = "Rad"
sector = "FLDK"

# TO CORRECT 
# available_products() 

# Filtering -> useful variables to test functions
import himawari_api.io as hi
info_dict = hi._get_info_from_filename("HS_H08_20210606_0050_B01_FLDK_R10_S0110.DAT.bz2")

satellite = info_dict["satellite"]
product = info_dict["product"]
start_time = info_dict["start_time"]
