#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep  7 14:48:54 2022

@author: ghiggi
"""
import dask
import satpy
import fsspec
import datetime
import numpy as np
from satpy import Scene, MultiScene
from satpy.readers import FSFile
from dask.diagnostics import ProgressBar
from goes_api import download_files, find_files
from dask.distributed import Client

client = Client(processes=True)
# ---------------------------------------------------------------------------.
# Install satpy and imageio-ffmpeg to execute this script
# conda install -c conda-forge satpy imageio-ffmpeg

# ---------------------------------------------------------------------------.
#### Temporary required


def get_channels_from_wavelengths(scn, wavelengths):
    dependency_tree = scn._dependency_tree
    list_names = []
    for band in wavelengths:
        dict_dataset_ids = dependency_tree._find_matching_ids_in_readers(band)
        for reader, data_ids in dict_dataset_ids.items():  # TODO: here I assume is only 1 reader
            channel = np.unique([data_id['name']
                                for data_id in data_ids]).tolist()
            list_names += channel
    return list_names


def _get_required_channels(scn, dataset):
    # Load 1 channel to enable dependency tree initialization
    dummy_dataset = scn.available_dataset_names()[0]
    scn.load([dummy_dataset])

    # Retrieve dependency tree
    dependency_tree = scn._dependency_tree
    compositor = dependency_tree.get_compositor(dataset)
    prerequisites = compositor.attrs['prerequisites']

    # Prerequisites can be
    # - satpy.dataset.DataQuery
    # - list of wavelengths
    # - satpy.node.ReaderNode (when dataset is a channel)

    # Convert wavelength to names if it is the case
    if isinstance(prerequisites[0], float):
        prerequisites = get_channels_from_wavelengths(scn, prerequisites)
    # Get valid channel names
    list_valid_channels = scn.all_dataset_names()
    # Retrive required channels
    list_channels = []
    for x in prerequisites:
        if isinstance(x, satpy.node.ReaderNode):  # channel
            channel = x.name['name']
            list_channels.append(channel)
        elif isinstance(x, str):
            list_channels.append(x)
        else:  # satpy.dataset.dataid.DataQuery
            name = x.get("name")
            if name not in list_valid_channels:  # is a composite
                channels = _get_required_channels(scn, name)
                list_channels += channels
            else:
                list_channels.append(name)
    list_channels = np.unique(list_channels).tolist()

    # Unload dummy id
    scn.unload([dummy_dataset])
    return list_channels


def get_required_channels(scn, datasets):
    if isinstance(datasets, str):
        datasets = [datasets]
    list_channels = []
    for dataset in datasets:
        channels = _get_required_channels(scn, dataset)
        list_channels += channels
    list_channels = np.unique(list_channels).tolist()
    return list_channels

# scn.all_composite_names()
# _get_required_channels(scn, "C01")
# _get_required_channels(scn, "true_color")
# _get_required_channels(scn, "cloud_phase_distinction")
# get_required_channels(scn, ["true_color", "C01", "cloud_phase_distinction"])
# get_required_channels(scn, composites)


# ---------------------------------------------------------------------------.
# Define protocol
base_dir = None

protocol = "gcs"
protocol = "s3"
fs_args = {}

# ---------------------------------------------------------------------------.
# Define satellite, sensor, product_level and product
satellite = "GOES-16"
sensor = "ABI"
product_level = "L1B"
product = "Rad"

# ---------------------------------------------------------------------------.
# Define time window, sector and filtering options
start_time = datetime.datetime(2017, 6, 28, 18, 0)
end_time = datetime.datetime(2017, 6, 29, 1, 0)
sector = "F"
scan_modes = None  # select all scan modes (M3, M4, M6)
scene_abbr = None  # M1 or M2
# select channels for True Color generation
channels = ["C01", "C02", "C03", "C05", "C13", "C14"]
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

# ---------------------------------------------------------------------------.
# Download files
base_dir = "/home/ghiggi/GEO"
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

# ---------------------------------------------------------------------------.
# Open files from s3 using ffspec + satpy
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

# List timesteps available
fpaths_dict = find_files(
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
list(fpaths_dict.keys())

ll_bbox = [-98, 38, -88, 45]  # [x_min, y_min, x_max, ymax]
composites = ["true_color", "green_snow", "cloud_phase_distinction"]

# -----------------------------------------------------------------------------.
# # Define Scene for each timestep
# scn_dict = {}
# for timestep, fpaths in fpaths_dict.items():
#     print(timestep)
#     # - Use satpy
#     scn = Scene(filenames=fpaths, reader="abi_l1b")
#     # - Load channels, crop, resample, create composite and unload
#     required_channels = get_required_channels(scn, composites)
#     scn.load(required_channels)
#     scn = scn.crop(ll_bbox=ll_bbox)
#     scn = scn.resample(resampler="native")
#     scn.load(composites)
#     scn.unload(required_channels)
#     # - Add to dictionary
#     scn_dict[timestep] = scn

# print("Number of Scenes: ", len(scn_dict))

# Define Scene for each timestep in parallel


@dask.delayed
def _delayed_scene(fpaths, reader, datasets, ll_bbox=None):
    # - Use satpy
    scn = Scene(filenames=fpaths, reader=reader)
    # - Load required channels
    required_channels = get_required_channels(scn, datasets)
    scn.load(required_channels)
    # - TODO: to speed up --> load channels before

    # - Crop
    if ll_bbox is not None:
        scn = scn.crop(ll_bbox=ll_bbox)
    # - Resample to common grid
    scn = scn.resample(resampler="native")
    # - Load datasets
    scn.load(datasets)
    return scn


scn_dict = {}
for timestep, fpaths in fpaths_dict.items():
    scn_dict[timestep] = _delayed_scene(fpaths, reader="abi_l1b",
                                        datasets=composites,
                                        ll_bbox=ll_bbox)

list_scenes = dask.compute(list(scn_dict.values()))[0]
timesteps = list(scn_dict.keys())
scn_dict = dict(zip(timesteps, list_scenes))

# ---------------------------------------------------------------------------.
# Generate an animation using satpy Multiscene functionalities
# - Create MultiScene
mscn = MultiScene(list(scn_dict.values()))
mscn.loaded_dataset_ids

# - Load composites if not yet loaded
# mscn.load(composites)

# - Create the animation
with ProgressBar():
    mscn.save_animation("/tmp/{name}_{start_time:%Y%m%d_%H%M%S}.mp4",
                        datasets=composites,
                        fps=2,
                        batch_size=4, client=client)

# ---------------------------------------------------------------------------.
# Close client 
client.close()
# ---------------------------------------------------------------------------.
# Installation required
# conda install imageio-ffmpeg
# sudo apt install gstreamer1.0-libav libavcodec-extra gstreamer1.0-plugins-ugly
# sudo apt-get install ubuntu-restricted-extras


# Create yml bbox ... for state, countries,
# https://anthonylouisdagostino.com/bounding-boxes-for-all-us-states/
# # iowa


# Get RGB xr.DataArray from MultiScene
# enhance ...
# https://github.com/pytroll/satpy/blob/main/satpy/etc/enhancements/generic.yaml 
# get_enhanced_images 


# da = scn['true_color']/255
# da.plot.imshow(x="x", y="y", rgb="bands")

# Save with geogif
# Save with ffmpeg

# Or geogif
# https://github.com/gjoseph92/geogif

#  enh_args={
# "decorate": {
#     "decorate": [
#         {"text": {
#             "txt": "time {start_time:%Y-%m-%d %H:%M}",
#             "align": {
#                 "top_bottom": "bottom",
#                 "left_right": "right"},
#             "font": '/usr/share/fonts/truetype/arial.ttf',
#             "font_size": 20,
#             "height": 30,
#             "bg": "black",
#             "bg_opacity": 255,
#             "line": "white"}}]}})
# ---------------------------------------------------------------------------.
