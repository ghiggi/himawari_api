#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 11:13:47 2022

@author: ghiggi
"""
import datetime
from goes_api import (
    find_closest_start_time,
    find_latest_start_time,
    find_files,
    find_closest_files,
    find_latest_files,
    find_previous_files,
    find_next_files,
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

sector = "M"
scene_abbr = ["M1"]  # None download and find both locations
scan_modes = None  # select all scan modes (M3, M4, M6)
channels = None  # select all channels
channels = ["C01", "C02"]  # select channels subset
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

####---------------------------------------------------------------------------.
#### Find files between start_time and end_time
fpaths = find_files(
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
    verbose=True,
)

print(fpaths)

####---------------------------------------------------------------------------.
#### Find files with start_time closest to a given time
fpaths = find_closest_files(
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    time=start_time,
    filter_parameters=filter_parameters,
)

print(fpaths)

####---------------------------------------------------------------------------.
#### Find N files prior/after given time
# 1. Find file start_time close to a given time
start_time = find_closest_start_time(
    time=start_time,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    protocol="s3",
    filter_parameters=filter_parameters,
)
# 2. Define retrieval settings
N = 5
include_start_time = False
check_consistency = True

# 3. Retrieve previous files
fpaths = find_previous_files(
    start_time=start_time,
    N=N,
    include_start_time=include_start_time,
    check_consistency=check_consistency,
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
)
print(fpaths)
assert len(fpaths) == N

# 4. Retrieve next files
fpaths = find_next_files(
    start_time=start_time,
    N=N,
    include_start_time=include_start_time,
    check_consistency=check_consistency,
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
)

print(fpaths)
assert len(fpaths) == N

####---------------------------------------------------------------------------.
#### Find latest available files
# - By default it retrieve just the last timestep available
N = 2
check_consistency = True

fpaths = find_latest_files(
    N=N,
    check_consistency=check_consistency,
    look_ahead_minutes=30,
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
)
print(fpaths)
assert len(fpaths) == N

####---------------------------------------------------------------------------.
