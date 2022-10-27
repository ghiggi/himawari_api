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
group_by_key = None

# Filtering -> useful variables to test functions
import himawari_api.io as hi
info_dict = hi._get_info_from_filename("HS_H08_20210606_0050_B01_FLDK_R10_S0110.DAT.bz2")

satellite = info_dict["satellite"]
product = info_dict["product"]
start_time = info_dict["start_time"]
