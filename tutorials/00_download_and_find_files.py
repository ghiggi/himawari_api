#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 14:59:57 2022

@author: ghiggi
"""
import datetime
import himawari_api
from himawari_api import download_files, find_files, group_files

###---------------------------------------------------------------------------.
#### An overview of himawari_api
print(dir(himawari_api))
himawari_api.available_protocols()

himawari_api.available_satellites()
himawari_api.available_product_levels()
himawari_api.available_products()
himawari_api.available_products("L1B")
himawari_api.available_products("L2")

himawari_api.available_sectors()
himawari_api.available_channels()


###---------------------------------------------------------------------------.
#### Define protocol and local directory
base_dir = "/tmp/"

protocol = "s3"
fs_args = {}

###---------------------------------------------------------------------------.
#### Define satellite, product_level and product
satellite = "HIMAWARI-8"
product_level = "L1B"
product = "Rad"

###---------------------------------------------------------------------------.
#### Define sector and filtering options
start_time = datetime.datetime(2021, 11, 17, 11, 30)
end_time = datetime.datetime(2021, 11, 17, 11, 40)

# - Full Disc Example
sector = "F"
scene_abbr = None  # DO NOT SPECIFY FOR FULL DISC SECTOR
channels = None  # select all channels
channels = ["C01"]  # select channels subset
filter_parameters = {}
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

# - Target Area Example
sector = "Target"
scene_abbr = None
channels = None  # select all channels
channels = ["C01"]  # select channels subset
filter_parameters = {}
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

###---------------------------------------------------------------------------.
#### Download files
n_threads = 20  # n_parallel downloads
force_download = False  # whether to overwrite existing data on disk

l_fpaths = download_files(
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
    n_threads=n_threads,
    force_download=force_download,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True,
)

###---------------------------------------------------------------------------.
#### Retrieve filepaths from local disk
fpaths = find_files(
    base_dir=base_dir,
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
assert fpaths == l_fpaths

###---------------------------------------------------------------------------.
#### Group filepaths by key (i.e. start_time)
himawari_api.available_group_keys()
fpath_dict = group_files(fpaths,  key="start_time")
print(fpath_dict)

# Alternatively specify the group_by_key args in find_files
fpath_dict = find_files(
    base_dir=base_dir,
    satellite=satellite,
    product_level=product_level,
    product=product,
    sector=sector,
    start_time=start_time,
    end_time=end_time,
    filter_parameters=filter_parameters,
    group_by_key="start_time",
    verbose=True,
)
print(fpath_dict)

###---------------------------------------------------------------------------.
#### Retrieve filepaths from cloud buckets
himawari_api.available_connection_types()

# Bucket url
fpaths = find_files(
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    product_level=product_level,
    product=product,
    sector=sector,
    start_time=start_time,
    end_time=end_time,
    filter_parameters=filter_parameters,
    connection_type="bucket",  # default
    verbose=True,
)
print(fpaths[0])

# https url
fpaths = find_files(
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    product_level=product_level,
    product=product,
    sector=sector,
    start_time=start_time,
    end_time=end_time,
    filter_parameters=filter_parameters,
    connection_type="https",
    verbose=True,
)
print(fpaths[0])

###---------------------------------------------------------------------------.
