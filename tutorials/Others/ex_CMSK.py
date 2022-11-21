#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 21 17:55:13 2022

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
product = "CMSK"       

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

# - CloudMask values  
pprint.pprint(ds['CloudMask'].attrs)

# - CloudMask Data Quality Flag  
pprint.pprint(ds['CloudMaskQualFlag'].attrs)

# - Plot CloudMask  
ds['CloudMask'].plot.imshow()
plt.show()
 
# - Plot CloudProbability
ds["CloudProbability"].plot.imshow()
plt.show()
 
# - Plot Smoke_Mask
ds["Smoke_Mask"].plot.imshow()
plt.show()

# - Plot Smoke_Mask
ds["Fire_Mask"].plot.imshow()
plt.show()

# - Plot Dust_Mask
ds["Dust_Mask"].plot.imshow()
plt.show()

