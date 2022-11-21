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
import subprocess
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
channels = None        # select all channels
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

# - Get list of available channels 
channels = scn.available_dataset_names()

# - Load channels
scn.load(channels)

# - Remap all channels to the lowest resolution
new_scn = scn.resample(scn.coarsest_area(), resampler="native")

# - Save each channels as separate png
new_scn.save_datasets(filename="/tmp/{name}.png")

###---------------------------------------------------------------------------.
# Use the ImageMagick command montage to join all the images together
cmd = "montage /tmp/B*.png -geometry 256x256 -background black /tmp/ahi-imager_mosaic.jpg"
subprocess.run(cmd, shell=True)
