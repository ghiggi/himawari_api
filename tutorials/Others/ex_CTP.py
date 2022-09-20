#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 17:13:06 2022

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
product = "CTP"     

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

# - Dataset Variables 
print(list(ds.data_vars))

# - DQF values  
ds['DQF'].attrs["flag_values"]
ds['DQF'].attrs["flag_meanings"]
np.unique(ds["DQF"].data[~np.isnan(ds["DQF"].data)], return_counts=True)  

# - Plot CTP field
ds['CTP'].plot.imshow()
plt.show()

# - Plot DQF
ds["DQF"].plot.imshow()
plt.show()
 
