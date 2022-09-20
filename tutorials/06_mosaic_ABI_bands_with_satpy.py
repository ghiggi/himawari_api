#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 18:17:58 2022

@author: ghiggi
"""
import subprocess
import fsspec
from satpy import Scene
from satpy.readers import FSFile
from goes_api import find_latest_files

###---------------------------------------------------------------------------.
# Install satpy to execute this script
# conda install -c conda-forge satpy

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
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

###---------------------------------------------------------------------------.
#### Open files from s3 using ffspec + satpy
fpaths_dict = find_latest_files(
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

# - Open files
fpaths = list(fpaths_dict.values())[0]
files = fsspec.open_files(fpaths, anon=True)

# - Define satpy FSFile
satpy_files = [FSFile(file) for file in files]

# - Use satpy
scn = Scene(filenames=satpy_files, reader="abi_l1b")
channels = scn.available_dataset_names()

# - Load channels
scn.load(channels)

# - Remap all channels to the lowest resolution
new_scn = scn.resample(scn.coarsest_area(), resampler="native")

# - Save each channels as separate png
new_scn.save_datasets(filename="/tmp/{name}.png")

###---------------------------------------------------------------------------.
# Use the ImageMagick command montage to join all the images together
cmd = "montage /tmp/C*.png -geometry 256x256 -background black /tmp/goes-imager_nc_mosaic.jpg"
subprocess.run(cmd, shell=True)
