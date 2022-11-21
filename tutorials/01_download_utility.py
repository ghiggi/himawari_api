#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Ghiggi Gionata

# himawari_api is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# himawari_api is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# himawari_api. If not, see <http://www.gnu.org/licenses/>.

import datetime
from himawari_api import (
    find_closest_start_time,
    download_files,
    download_previous_files,
    download_next_files,
    download_latest_files,
)

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

sector = "Target"
scene_abbr = None  # None download and find both locations
channels = None    # select all channels
channels = ["B04", "B05"]  # select channels subset
filter_parameters = {}
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
