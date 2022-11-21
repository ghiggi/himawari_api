#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 13:19:38 2022

@author: ghiggi
"""
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
sector = "Target"
scene_abbr = None   
channels = None  # select all channels
channels = ["C01", "C02", "C03"]  # select channels subset
filter_parameters = {}
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

###---------------------------------------------------------------------------.
#### Download latest files 
N = 2
n_threads = 4
force_download = True
check_consistency = True

fpaths_dict = himawari_api.download_latest_files(
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

