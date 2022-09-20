#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 13:19:38 2022

@author: ghiggi
"""
import fsspec
from satpy import Scene
from satpy.readers import FSFile
from goes_api import find_latest_files

###---------------------------------------------------------------------------.
# Install satpy to execute this script
# conda install -c conda-forge satpy

# NOTE: For GOES performance benchmarks, check https://github.com/ghiggi/goes_benchmarks/
###---------------------------------------------------------------------------.
#### Define protocol
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
sector = "M"
scene_abbr = ["M1"]  # None download and find both locations
scan_modes = None  # select all scan modes (M3, M4, M6)
channels = None  # select all channels
channels = ["C01", "C02", "C03"]  # select channels subset
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

###---------------------------------------------------------------------------.
#### Open file from s3 using ffspec + satpy
fpaths = find_latest_files(
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
    connection_type="bucket",
)
# - Retrieve bucket url
fpaths = list(fpaths.values())[0]

# - Open files
files = fsspec.open_files(fpaths, anon=True)
type(files)  # fsspec.core.OpenFiles
type(files[0])  # fsspec.core.OpenFile
files[0].full_name

# - Define satpy FSFile
satpy_files = [FSFile(file) for file in files]

# - Use satpy
scn = Scene(filenames=satpy_files, reader="abi_l1b")
scn.available_dataset_names()
scn.available_composite_names()

# - Display a channel
scn.load(scn.available_dataset_names())
scn.show("C01")

# - Display a composite [high-res]
new_scn = scn.resample(scn.finest_area(), resampler="native")
new_scn.load(["true_color"])
new_scn.show("true_color")

# - Display a composite [low-res]
new_scn = scn.resample(scn.coarsest_area(), resampler="native")
new_scn.load(["true_color"])
new_scn.show("true_color")
# new_scn.save_datasets(filename='{name}.png')

del files, satpy_files  # GOOD PRACTICE TO CLOSE CONNECTIONS !!!

###---------------------------------------------------------------------------.
#### Define custom blockcache
# - It download only the required parts of the remote file which are accessed
# - LRU cache/store the last N (default 32) blocks of file accessed.
# - Example borrowed from:
#   - https://github.com/gerritholl/sattools/blob/master/src/sattools/abi.py
#   - https://github.com/pytroll/satpy/pull/1321
# - Docs: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.cached.CachingFileSystem
# - For more subtleties: https://github.com/pytroll/satpy/pull/1321

import appdirs
from fsspec.implementations.cached import CachingFileSystem

# - Define cache
cachedir = appdirs.user_cache_dir("ABI-block-cache")
storage_options = {"anon": True}
fs_s3 = fsspec.filesystem(protocol="s3", **storage_options)
fs_block = CachingFileSystem(
    fs=fs_s3,
    cache_storage=cachedir,
    cache_check=600,
    check_files=False,
    expiry_times=False,
    same_names=False,
)

# - Define satpy FSFile
# --> TODO: PR to pass block_size to FSFile
satpy_files = [FSFile(fpath, fs=fs_block) for fpath in fpaths]
scn = Scene(filenames=satpy_files, reader="abi_l1b")

# - Display a channel
scn.load(scn.available_dataset_names())
scn.show("C01")

del satpy_files  # GOOD PRACTICE TO CLOSE CONNECTIONS !!!

###---------------------------------------------------------------------------.
#### Define custom simplecache
# - Cache/stores the entire file locally on first access
#   --> Subsequent reads are done locally
# - Example borrowed from:
#   - https://github.com/gerritholl/sattools/blob/master/src/sattools/abi.py
#   - https://github.com/pytroll/satpy/pull/1321
# - Docs: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.cached.SimpleCacheFileSystem

import appdirs
from fsspec.implementations.cached import SimpleCacheFileSystem

# - Define cache
cachedir = appdirs.user_cache_dir("ABI-simple-cache")
fs_simple = SimpleCacheFileSystem(
    fs=fs_s3,
    cache_storage=cachedir,
    check_files=False,
    expiry_times=False,
    same_names=True,
)

# - Define satpy FSFile
satpy_files = [FSFile(fpath, fs=fs_simple) for fpath in fpaths]
scn = Scene(filenames=satpy_files, reader="abi_l1b")
# - Display a channel
scn.load(scn.available_dataset_names())
scn.show("C01")

del satpy_files  # GOOD PRACTICE TO CLOSE CONNECTIONS !!!

###---------------------------------------------------------------------------.
