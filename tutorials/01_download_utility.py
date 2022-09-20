#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 11:13:47 2022

@author: ghiggi
"""
import datetime
from goes_api import (
    find_closest_start_time,
    download_files,
    download_previous_files,
    download_next_files,
    download_latest_files,
)

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
#### Download files between start_time and end_time
n_threads = 20  # n_parallel downloads
force_download = False  # whether to overwrite existing data on disk

fpaths = download_files(
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

print(fpaths)

####---------------------------------------------------------------------------.
#### Download N files prior/after given time
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
fpaths = download_previous_files(
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
    n_threads=n_threads,
    force_download=force_download,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True,
)
print(fpaths)
assert len(fpaths) == N

# 4. Retrieve next files
fpaths = download_next_files(
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
    n_threads=n_threads,
    force_download=check_consistency,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True,
)

print(fpaths)
assert len(fpaths) == N

####---------------------------------------------------------------------------.
#### Download latest files
# - By default it retrieve just the last timestep available
N = 2
check_consistency = True

fpaths = download_latest_files(
    N=N,
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
    n_threads=n_threads,
    force_download=force_download,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True,
)
print(fpaths)
assert len(fpaths) == N

####---------------------------------------------------------------------------.
# Visualize mesoscale image
import xarray as xr

fpath = list(fpaths.values())[0][0]
ds = xr.open_dataset(fpath)
ds["Rad"].plot.imshow()

####---------------------------------------------------------------------------.
