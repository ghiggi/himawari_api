#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 17:34:43 2022

@author: ghiggi
"""
import numpy as np
import requests
import xarray as xr
from io import BytesIO
import matplotlib.pyplot as plt
from goes_api import find_latest_files

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
product_level = "L2"
product = "LVMP"     

###---------------------------------------------------------------------------.
#### Define sector and filtering options
sector = "F"
scan_modes = None   # select all scan modes (M3, M4, M6)
channels = None     # select all channels
scene_abbr = None   
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

#----------------------------------------------------------------------------.
#### Open file using in-memory buffering via https requests  
fpaths = find_latest_files(
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    sensor=sensor,
    product_level=product_level,
    product=product,
    sector=sector,
    filter_parameters=filter_parameters,
    connection_type="https",
)
# - Select http url
fpath = list(fpaths.values())[0][0]
print(fpath)

# - Open the dataset 
resp = requests.get(fpath)
f_obj = BytesIO(resp.content)
ds = xr.open_dataset(f_obj)
print(ds)

# - Dataset Name 
print(ds.title)

# - Dataset Resolution
print(ds.attrs["spatial_resolution"]) # 10 km at nadirs

# - Dataset Attributes
print(ds.attrs["summary"])

# - Pressure Levels 
print(ds.pressure.data)

# - Dataset Variables 
print(list(ds.data_vars))

# - DQF values  
ds['DQF_Overall'].attrs["flag_values"]
ds['DQF_Overall'].attrs["flag_meanings"]
np.unique(ds["DQF_Overall"].data[~np.isnan(ds["DQF_Overall"].data)], return_counts=True)  

ds['DQF_Retrieval'].attrs["flag_values"]
ds['DQF_Retrieval'].attrs["flag_meanings"]
np.unique(ds["DQF_Retrieval"].data[~np.isnan(ds["DQF_Retrieval"].data)], return_counts=True)  

ds['DQF_SkinTemp'].attrs["flag_values"]
ds['DQF_SkinTemp'].attrs["flag_meanings"]
np.unique(ds["DQF_SkinTemp"].data[~np.isnan(ds["DQF_SkinTemp"].data)], return_counts=True)  

# - Plot LVM field
ds['LVM'].isel(pressure=0).plot.imshow(cmap="Spectral")
plt.show()
ds['LVM'].isel(pressure=50).plot.imshow(cmap="Spectral")
plt.show()
ds['LVM'].isel(pressure=100).plot.imshow(cmap="Spectral")
plt.show()

# - Plot DQF
ds["DQF_Overall"].plot.imshow()
plt.show()
 
ds["DQF_Retrieval"].plot.imshow()
plt.show()

ds["DQF_SkinTemp"].plot.imshow()
plt.show()
