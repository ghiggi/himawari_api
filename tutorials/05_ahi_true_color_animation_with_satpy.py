#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 17:36:34 2022

@author: ghiggi
"""
import datetime
import himawari_api
from satpy import Scene, MultiScene
from dask.diagnostics import ProgressBar

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


###---------------------------------------------------------------------------.
#### Define satpy Scene for each timestep and add to a dictionary 
scn_dict = {}
for timestep, fpaths in fpaths_dict.items():
    # Load Scene 
    scn = Scene(filenames=fpaths, reader="ahi_hsd")
    # Load the channels  
    scn.load(["B01","B02", "B03"])
    # Resample chanels to common grid 
    scn = scn.resample(scn.finest_area(), resampler="native")
    # Create the composite  
    scn.load(["true_color_raw"])
    scn_dict[timestep] = scn

print("Number of Scenes: ", len(scn_dict))

###---------------------------------------------------------------------------.
#### Generate an animation using satpy Multiscene functionalities
# - Create MultiScene
mscn = MultiScene(scn_dict.values())
# - Create the animation
with ProgressBar():
    mscn.save_animation(filename="/tmp/{name}_{start_time:%Y%m%d_%H%M%S}.mp4", 
                        datasets=["true_color_raw"], 
                        fps=5) # batch_size=4)

###---------------------------------------------------------------------------.