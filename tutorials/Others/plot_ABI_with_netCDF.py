#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 10:45:37 2022

@author: ghiggi
"""
import netCDF4
import requests
import cartopy
import cartopy.crs as ccrs
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
product_level = "L1B"
product = "Rad"

###---------------------------------------------------------------------------.
#### Define sector and filtering options
sector = "F"
scan_modes = None   # select all scan modes (M3, M4, M6)
channels = None     # select all channels
channels = ["C01"]  # select channels subset
scene_abbr = None   
filter_parameters = {}
filter_parameters["scan_modes"] = scan_modes
filter_parameters["channels"] = channels
filter_parameters["scene_abbr"] = scene_abbr

#----------------------------------------------------------------------------.
#### Open file using in-memory buffering via https requests and netCDF4
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

# - Open netCDF4 (via https connection)
resp = requests.get(fpath)
nc_ds = netCDF4.Dataset("whatevername", memory=resp.content)

# Dataset Name 
nc_ds.title

# Radiance array 
arr = nc_ds['Rad'][:]

#----------------------------------------------------------------------------.
#### Retrieve GEOS projection x and y coordinates 
# - GEOS projection coordinates equals the scanning angle (in radians) 
#   multiplied by the satellite height
# - GEOS projection: (http://proj4.org/projections/geos.html)

# Satellite height
sat_h = nc_ds.variables['goes_imager_projection'].perspective_point_height

# Satellite longitude
sat_lon = nc_ds.variables['goes_imager_projection'].longitude_of_projection_origin

# Satellite sweep
sat_sweep = nc_ds.variables['goes_imager_projection'].sweep_angle_axis

# Retrieve X and Y projection coordinates array 
X = nc_ds.variables['x'][:] * sat_h
Y = nc_ds.variables['y'][:] * sat_h

#----------------------------------------------------------------------------.
#### Plot with matplotlib and cartopy 
# - Define cartopy Geostationary projection 
crs_proj = ccrs.Geostationary(satellite_height=sat_h, 
                              central_longitude=sat_lon,
                              sweep_axis='x')

# - Create figure
fig, ax = plt.subplots(figsize=(10, 10), subplot_kw={'projection': crs_proj})
 
# - Plot radiance array
p = ax.pcolorfast(X, Y, arr, cmap='Spectral', zorder=0, vmin=10)

# - Add coastlines
ax.add_feature(cartopy.feature.COASTLINE, zorder=1, color='b', lw=1)

# - Add title
ax.set_title('GOES16', fontweight='bold', fontsize=12)

# - Add colorbar 
cbar = plt.colorbar(p, ax=ax, shrink=0.75)
cbar.set_label('Radiance',fontsize=12)
cbar.ax.tick_params(labelsize=10)

# - Set tight layout 
plt.tight_layout()

#----------------------------------------------------------------------------.
 