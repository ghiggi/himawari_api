#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 13:58:23 2022

@author: ghiggi
"""
import datetime
import himawari_api.query
from himawari_api import (
    find_files,
    group_files,
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
#### Define satellite, product_level and product
satellite = "HIMAWARI-8"
product_level = "L1B"
product = "Rad"

###---------------------------------------------------------------------------.
#### Define sector and filtering options
start_time = datetime.datetime(2020, 11, 17, 11, 30)
end_time = datetime.datetime(2020, 11, 17, 11, 40)

sector = "Target"
scene_abbr = None  
channels = None  # select all channels
channels = ["C01", "C02"]  # select channels subset
filter_parameters = {}
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

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
    verbose=True,
)

print(fpaths)

####---------------------------------------------------------------------------.
#### Query filepaths 
# - List of filepaths 
himawari_api.query.start_time(fpaths)
himawari_api.query.end_time(fpaths)
himawari_api.query.product_level(fpaths)
himawari_api.query.product(fpaths)
himawari_api.query.sector(fpaths)
himawari_api.query.scene_abbr(fpaths)
himawari_api.query.channel(fpaths)
himawari_api.query.satellite(fpaths)

# - Dictionary with list of filepaths 
fpaths_dict =  group_files(fpaths, key="start_time")
print(fpaths_dict)
himawari_api.query.start_time(fpaths_dict)
himawari_api.query.end_time(fpaths_dict)
himawari_api.query.product_level(fpaths_dict)
himawari_api.query.product(fpaths_dict)
himawari_api.query.sector(fpaths_dict)
himawari_api.query.scene_abbr(fpaths_dict)
himawari_api.query.channel(fpaths_dict)
himawari_api.query.satellite(fpaths_dict)


 