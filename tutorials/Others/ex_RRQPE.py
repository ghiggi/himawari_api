#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 17:23:36 2022

@author: ghiggi
"""
import pprint
import datetime
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from himawari_api import download_files

###---------------------------------------------------------------------------.
#### Define protocol and local directory
base_dir = "/tmp/"
protocol = "s3"
fs_args = {}

###---------------------------------------------------------------------------.
#### Define satellite, product_level and product
satellite = "HIMAWARI-8"
product_level = "L2"
product = "RRQPE"      # RRQPE, CMSK, CPHS, CHGT

###---------------------------------------------------------------------------.
#### Define sector and time period 
sector = "FLDK" 
start_time = datetime.datetime(2021, 11, 17, 21, 30)
end_time = datetime.datetime(2021, 11, 17, 21, 40)

#----------------------------------------------------------------------------.
#### Download files between start_time and end_time
n_threads = 4  # n_parallel downloads
force_download = False  # whether to overwrite existing data on disk

fpaths = download_files(
    base_dir=base_dir,
    protocol=protocol,
    fs_args=fs_args,
    satellite=satellite,
    product_level=product_level,
    product=product,
    sector=sector,
    start_time=start_time,
    end_time=end_time,
    n_threads=n_threads,
    force_download=force_download,
    check_data_integrity=True,
    progress_bar=True,
    verbose=True,
)

print(fpaths)

# Select 1 file 
fpath = fpaths[0]

#----------------------------------------------------------------------------.
#### Open L2 netCDF with xarray
ds = xr.open_dataset(fpath)
print(ds)

# - Dataset Name 
print(ds.title)

# - Dataset Attributes
pprint.pprint(ds.attrs)

# - Dataset Resolution
print(ds.attrs["resolution"]) # '2km at nadir',

# - Dataset Summary
print(ds.attrs["summary"])

# - Dataset Variables 
print(list(ds.data_vars))

# - DQF values  
# --> [0, 2, 3, 64, 66]
pprint.pprint(ds['DQF'].attrs)
np.unique(ds["DQF"].data[~np.isnan(ds["DQF"].data)], return_counts=True)  

# - RRPQPE values  
pprint.pprint(ds['RRQPE'].attrs)
np.unique(ds["RRQPE"].data[~np.isnan(ds["RRQPE"].data)], return_counts=True)  

# - Plot RRQPE field
da = ds['RRQPE'] 
da = da.where(da > 0) # Mask with nan where <=0
da.plot.imshow()
plt.show()
 
# - Plot DQF
ds["DQF"].plot.imshow()
plt.show()
 


