#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 26 15:38:44 2022

@author: ghiggi
"""
import datetime
from himawari_api import (
    find_files,
)

###---------------------------------------------------------------------------.
#### Define protocol or local directory
# - If base_dir specified
# --> Search on local storage
# --> Protocol must be None (or "file" ... see ffspec.LocalFileSystem)

# - If protocol is specified
# --> base_dir must be None
# --> Search on cloud bucket

base_dir = None

protocol = "s3"
fs_args = {}

###---------------------------------------------------------------------------.
#### Define satellite, sensor, product_level and product
satellite = "HIMAWARI-8"
product_level = "L1B"
product = "Rad"

###---------------------------------------------------------------------------.
#### Define sector and filtering options
start_time = datetime.datetime(2021, 11, 17, 11, 30)
end_time = datetime.datetime(2021, 11, 17, 11, 50)

sector = "Japan" # "F"          
scene_abbr = None          # "R1"   
channels = None            # select all channels
channels = ["C01", "C02"]  # select channels subset
filter_parameters = {}
filter_parameters["channels"] = channels
# filter_parameters["scene_abbr"] = scene_abbr
verbose = True

####---------------------------------------------------------------------------.
#### Find files between start_time and end_time
fpaths = find_files(
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    product_level=product_level,
    product=product,
    sector=sector,
    start_time=start_time,
    end_time=end_time,
    filter_parameters=filter_parameters,
    group_by_key="start_time",
    verbose=verbose,
)

print(fpaths)

fname = 'HS_H08_20211117_1130_B01_FLDK_R10_S0110.DAT.bz2'
connection_type = None


# Add a function that for each timestep take only the highest resolution per band file  
# Use group_by_key="start_time"
# Then, use "channel" and "segment_number" from the info_dict to identify if duplicate band file 
# Select the one with higher resolution using "spatial_res" (5, 10, 20)

# TODO
# group_by_file (dictionary with filename in the key, and the corresponding filepaths of each of the segments)

 

