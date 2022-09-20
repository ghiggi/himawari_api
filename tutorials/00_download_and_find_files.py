#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 14:59:57 2022

@author: ghiggi
"""
import datetime
import goes_api
from goes_api import download_files, find_files, group_files

###---------------------------------------------------------------------------.
#### An overview of goes_api
print(dir(goes_api))
goes_api.available_protocols()

goes_api.available_satellites()
goes_api.available_sensors()
goes_api.available_product_levels("ABI")
goes_api.available_products("ABI")
goes_api.available_products("ABI", "L1B")
goes_api.available_products("ABI", "L2")

goes_api.available_sectors()
goes_api.available_scan_modes()
goes_api.available_channels()

## List online GOES-16 netCDF data
from goes_api.io import get_available_online_product

get_available_online_product(protocol="s3", satellite="goes-16")
get_available_online_product(protocol="gcs", satellite="goes-16")

###---------------------------------------------------------------------------.
#### Define protocol and local directory
base_dir = "/tmp/"

protocol = "gcs"
protocol = "s3"
fs_args = {}

###---------------------------------------------------------------------------.
#### Define satellite, sensor, product_level and product
satellite = "GOES-16"
sensor = "ABI"
product_level = "L1B"
product = "Rad"

###---------------------------------------------------------------------------.
#### Define sector and filtering options
start_time = datetime.datetime(2019, 11, 17, 11, 30)
end_time = datetime.datetime(2019, 11, 17, 11, 40)

# - Full Disc Example
sector = "F"
scene_abbr = None  # DO NOT SPECIFY FOR FULL DISC SECTOR
scan_modes = None  # select all scan modes (M3, M4, M6)
channels = None  # select all channels
channels = ["C01"]  # select channels subset
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

# - Mesoscale Example
sector = "M"
scene_abbr = ["M1"]  # None download and find both locations
scan_modes = None  # select all scan modes (M3, M4, M6)
channels = None  # select all channels
channels = ["C01"]  # select channels subset
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
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
    sensor=sensor,
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
    sensor=sensor,
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
goes_api.available_group_keys()
fpath_dict = group_files(fpaths,  key="start_time")
print(fpath_dict)

# Alternatively specify the group_by_key args in find_files
fpath_dict = find_files(
    base_dir=base_dir,
    satellite=satellite,
    sensor=sensor,
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
goes_api.available_connection_types()

# Bucket url
fpaths = find_files(
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
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
    sensor=sensor,
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
