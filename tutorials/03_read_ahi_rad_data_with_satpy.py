#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 21 17:16:34 2022

@author: ghiggi
"""
import datetime
import himawari_api
from satpy import Scene

###---------------------------------------------------------------------------.
# Install satpy to execute this script
# conda install -c conda-forge satpy

###---------------------------------------------------------------------------.
#### Define protocol
base_dir = "/tmp/"
protocol = "s3"
fs_args = {}

###---------------------------------------------------------------------------.
#### Define satellite, sensor, product_level and product
satellite = "HIMAWARI-8"
product_level = "L1B"
product = "Rad"

###---------------------------------------------------------------------------.
#### Define sector and filtering options
start_time = datetime.datetime(2021, 11, 17, 21, 30)
end_time = datetime.datetime(2021, 11, 17, 21, 40)

sector = "Target"
scene_abbr = None   
channels = None  # select all channels
channels = ["C01", "C02", "C03"]  # select channels subset
filter_parameters = {}
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

###---------------------------------------------------------------------------.
#### Download files 
n_threads = 4
force_download = True

fpaths = himawari_api.download_files(
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

# Group by timesteps
fpaths_dict = himawari_api.group_files(fpaths, key="start_time")
print(fpaths_dict)

# Look at available timesteps
list_timesteps = list(fpaths_dict.keys())

# Select filepath for a single timestep 
fpaths = fpaths_dict[list_timesteps[0]]

###---------------------------------------------------------------------------.
#### Open files with satpy
scn = Scene(filenames=fpaths, reader="ahi_hsd")
scn.available_dataset_names()
scn.available_composite_names()

# - Display a channel
scn.load(scn.available_dataset_names())
scn.show("B01")

# - Display a composite [high-res]
new_scn = scn.resample(scn.finest_area(), resampler="native")
new_scn.load(["true_color_raw"])
new_scn.show("true_color_raw")

# - Display a composite [low-res]
new_scn = scn.resample(scn.coarsest_area(), resampler="native")
new_scn.load(["true_color_raw"])
new_scn.show("true_color_raw")

# new_scn.save_datasets(filename='{name}.png')

