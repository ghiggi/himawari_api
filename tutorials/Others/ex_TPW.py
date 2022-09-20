#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 16:40:40 2022

@author: ghiggi
"""
import numpy as np
import requests
import xarray as xr
import matplotlib.pyplot as plt 
from io import BytesIO
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
product = "TPW"     # 10 km at nadir 

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
print(ds.attrs["spatial_resolution"]) # 10 km at nadir 

# - Dataset Variables 
print(list(ds.data_vars))

# - DQF values (excluded NaNs)
np.unique(ds["DQF_SkinTemp"].data[~np.isnan(ds["DQF_SkinTemp"].data)], return_counts=True)
np.unique(ds["DQF_Retrieval"].data[~np.isnan(ds["DQF_Retrieval"].data)], return_counts=True)
np.unique(ds["DQF_Overall"].data[~np.isnan(ds["DQF_Overall"].data)], return_counts=True)

# - Plot TPW field
ds['TPW'].plot.imshow()
plt.show()

# - Plot DQF
ds["DQF_SkinTemp"].plot.imshow()
plt.show()

ds["DQF_Retrieval"].plot.imshow()
plt.show()

ds["DQF_Overall"].plot.imshow()
plt.show()
