#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Ghiggi Gionata, Léo Jacquat

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

#-----------------------------------------------------------------------------.
# Himawari products page : https://www.eorc.jaxa.jp/ptree/userguide.html

# L1B is in Himawari Standard Format (HSD)
# --> Satpy Reader: https://satpy.readthedocs.io/en/stable/api/satpy.readers.ahi_hsd.html    

# The AHI-L1b-FLDK directory contains:
# - The AHI L1B radiance data over the Full Disc every 10 minutes 

# The AHI-L1b-Japan directory contains:
# - The AHI L1B radiance data over Japan every 2.5 minutes 
# - Observation numbers: JP01 to JP04  

# The AHI-L1b-Target directory contains: 
# - The AHI L1B radiance data over the movable sector every 2.5 minutes 
# - Observation numbers: R301 to R304

# The LandMark (Area) directory is not available on AWS
 
# L2 products all in netCDF format 
# - himawari_api allows to retrieve only the Clouds and RainfallRate files. 

#-----------------------------------------------------------------------------.
# Other files available on AWS
# ISatSS: 
# - Same info as L1B HSD
# - Tiled netCDF for AWIPS software

# FLDK-SST are only available hourly 
# - GeoProjection: L2*
# - Gridded Lat,Lon: L3*

# Himawari products as presented in AWS : https://noaa-himawari8.s3.amazonaws.com/README.txt
#-----------------------------------------------------------------------------.
# Other existing AHI L2 products not available on AWS: 
# - https://www.eorc.jaxa.jp/ptree/userguide.html
# "AP": "Aerosol Property", # Full Disk, daytime only
# "CP": "Cloud Property",   # Full disk, daytime only
# "SSTN": "Sea Surface Temperature (Nighttime mode)", # Full Disk
# "SWR": "Short Wave Radiation / Photosynthetically Available Radiation", # Full Disk
# "ChA": "Chlorophyll-a",  # Full disk
# "WF": "Wild Fires",

#-----------------------------------------------------------------------------.
    
AHI_L1b_PRODUCTS = {
    "Rad": "Radiances",          # AHI-L1b-FLDK/ , AHI-L1b-Japan/ , AHI-L1b-Target/
}

AHI_L2_PRODUCTS = {
    "RRQPE": "Rainfall Rate",    # AHI-L2-FLDK-RainfallRate/
    "CMSK": "Cloud Mask",        # AHI-L2-FLDK-Clouds/
    "CPHS": "Cloud Top Phase",   # AHI-L2-FLDK-Clouds/
    "CHGT": "Cloud Top Height",  # AHI-L2-FLDK-Clouds/
    }


AHI_L2_SECTOR_EXCEPTIONS = {
    "RRQPE": ["FLDK"],
    "CMSK": ["FLDK"],
    "CPHS": ["FLDK"],
    "CHGT": ["FLDK"],
}
 
PRODUCTS = {
    "AHI": {
        "L1b": AHI_L1b_PRODUCTS,
        "L2": AHI_L2_PRODUCTS,
    },
}

 
GLOB_FNAME_PATTERN = {
    "AHI": {

        "L1b": { 
            "Rad": 'HS_{platform_shortname:3s}_{start_time:%Y%m%d_%H%M}_{channel:3s}_{sector_observation_number:4s}_R{spatial_res:2d}_S{segment_number:2d}{segment_total:2d}.{data_format}',  
        },
        "L2": {
            # PRODUCT = [CLOUD_MASK, CMSK]
            "CLOUD_MASK": "{platform_fullname}_{sensor:3s}_{sector:4s}_{start_time:%Y%j_%H%M_%S}_{product}_EN.nc", # Before 2021
            "CMSK": "{sensor:3s}-{product}_{version}_{platform_shortname:3s}_s{start_time:%Y%m%d%H%M%S%f}_e{end_time:%Y%m%d%H%M%S%f}_c{production_time:%Y%m%d%H%M%S%f}.nc",
            # PRODUCT = [CLOUD_PHASE, CPHS]
            "CLOUD_PHASE": "{platform_fullname}_{sensor:3s}_{sector:4s}_{start_time:%Y%j_%H%M_%S}_{product}_EN.nc", # Before 2021
            "CPHS": "{sensor:3s}-{product}_{version}_{platform_shortname:3s}_s{start_time:%Y%m%d%H%M%S%f}_e{end_time:%Y%m%d%H%M%S%f}_c{production_time:%Y%m%d%H%M%S%f}.nc",
            # PRODUCT = [CLOUD_HEIGHT, CHGT]
            "CLOUD_HEIGHT": "{platform_fullname}_{sensor:3s}_{sector:4s}_{start_time:%Y%j_%H%M_%S}_{product}_EN.nc", # Before 2021
            "CHGT": "{sensor:3s}-{product}_{version}_{platform_shortname:3s}_s{start_time:%Y%m%d%H%M%S%f}_e{end_time:%Y%m%d%H%M%S%f}_c{production_time:%Y%m%d%H%M%S%f}.nc",
            # PRODUCT = [HYDRO_RAIN_RATE, RRQPE]
            "HYDRO_RAIN_RATE": "{platform_fullname}_{sensor:3s}_2KM_{sector:4s}_{start_time:%Y%j_%H%M_%S}_{product}_EN.nc", # Before 2021
            "RRQPE": "{product}-{sensor:3s}-INST_{version}_{platform_shortname:3s}_s{start_time:%Y%m%d%H%M%S%f}_e{end_time:%Y%m%d%H%M%S%f}_c{production_time:%Y%m%d%H%M%S%f}.nc",
            
            },       
    },
}

