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

# The Japan directory contains
# - the data over Japan every 10 minutes 
# - JP01 to JP04
# - ATTENTION: Natively should be every 2.5 minutes   

# The Target (Area) directory contains 
# - the data over the movable sector every 10 minutes 
# - R301 to R304
# - ATTENTION: Natively should be every 0.5/2.5 minutes 
# - ATTENTION: Regions 4 and 5 missing 
# ---> LandMark Area not available on AWS
 
# ISatSS: 
# - Same info as L1B HSD
# - Tiled netCDF for AWIPS software

# L2 products all in netCDF format 

# FLDK-SST are only available hourly 
# - GeoProjection: L2*
# - Gridded Lat,Lon: L3*

#-----------------------------------------------------------------------------.
# TODO ? 
# To get the correct bucket directory, you will need to modify 
# https://github.com/ghiggi/himawari_api/blob/main/himawari_api/io.py#L808

# if product in ["CMSK", "CPHS", "CHGT"]
#     product_name = f"AHI-{product_level}-FLDK-Clouds"
# elif ... 

#-----------------------------------------------------------------------------.

AHI_L1b_PRODUCTS = {
    "RAD": "Radiances",
    # "FLDK": "Full Disk", 
    # "JP": "Japan Area", # They are sectors, not products
    # "TA": "Target Area"
}

# Existing K2 products not available on AWS: 
# - https://www.eorc.jaxa.jp/ptree/userguide.html
# "AP": "Aerosol Property", # Full Disk, daytime only
# "CP": "Cloud Property",   # Full disk, daytime only
# "SSTN": "Sea Surface Temperature (Nighttime mode)", # Full Disk
# "SWR": "Short Wave Radiation / Photosynthetically Available Radiation", # Full Disk
# "ChA": "Chlorophyll-a",  # Full disk
# "WF": "Wild Fires",

# Himawari products as presented in AWS : https://noaa-himawari8.s3.amazonaws.com/README.txt
AHI_L2_PRODUCTS = {
    #"ISatSS": "Cloud Moisture Imagery",
    #"CMI": "Cloud Moisture Imagery - R3 tiles",
    #"SST": "Sea Surface Temperature",
    #"NDMW": "Derive Motion Winds",
    
    
    "RRQPE": "Rainfall Rate",
    # Within the FLDK-Clouds
    "CMSK": "Cloud Mask",
    "CPHS": "Cloud Top Phase",
    "CHGT": "Cloud Top Height",
    }


AHI_L2_SECTOR_EXCEPTIONS = {
    #"SST": ["FLDK"], 
    #"NDMW": ["FLDK"],
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

        "L1b":{ 
            "RAD":{
                'HS_{platform_shortname:3s}_{start_time:%Y%m%d_%H%M}_B{band_number:2d}_{sector:4s}_R{spatial_res:2d}_S{segment_number:2d}{segment_total:2d}.{data_format}',
                }      
            },
        # sector:
        # FLDK -> full disk
        # JP{area_number:2s} Japan Area -> 01-04
        # R3{area_number:2s} Region 3 (Target Area) -> 01-04
        # R4{area_number:2s} Region 4 (Landmark Area) -> 01-20
        # R5{area_number:2s} Region 5 (Landmark Area) -> 01-20
        # --> We can retrieve more specific info if needed 
        #  modifying https://github.com/ghiggi/himawari_api/blob/main/himawari_api/io.py#L920
        "L2": {
            "CLOUD_MASK": { # PRODUCT = [CLOUD_MASK, CMSK]
                "{platform}_AHI_{sector:4s}_{start_time:%Y%j_%H%M_%S}_{product}_EN.nc", # Before 2021
                },
            "CMSK": {
                "AHI-{product}_{version}_{platform_shortname:3s}_s{start_time:%Y%m%d%H%M%S}_e{end_time:%Y%m%d%H%M%S}_c{production_time:%Y%m%d%H%M%S}.nc"
                },
            "CLOUD_PHASE": {
                "Himawari8_AHI_{sector:4s}_{start_time:%Y%j_%H%M_%S}_CLOUD_PHASE_EN.nc"
                }, # Before 2021
            "CPHS": {
                "AHI-CPHS_v1r0_h08_s{start_time:%Y%m%d%H%M%S}_e {end_time:%Y%m%d%H%M%S}_c{production_time:%Y%m%d%H%M%S}.nc"
                },
            "CLOUD_HEIGHT": {
                "Himawari8_AHI_{sector:4s}_{start_time:%Y%j_%H%M_%S}_CLOUD_HEIGHT_EN.nc", # Before 2021
                },
            "CHGT": {
                "AHI-CHGT_v1r0_h08_s{start_time:%Y%m%d%H%M%S}_e {end_time:%Y%m%d%H%M%S}_c{production_time:%Y%m%d%H%M%S}.nc"
                },
            "HYDRO_RAIN_RATE": {
                "{platform}_AHI_2KM_{sector:4s}_{start_time:%Y%j_%H%M_%S}_{product}_EN.nc", # Before 2021
                },
            "RRQPE": { # PRODUCT = [HYDRO_RAIN_RATE, RRQPE]
                "{product}-AHI-INST_{version}_{platform_shortname:3s}_{start_time:%Y%m%d%H%M%S}_e{end_time:%Y%m%d%H%M%S}_c{production_time:%Y%m%d%H%M%S}.nc"
                },
            
            # The following we don't need to implement 
            # Already doing l2-Clouds and L1B-Rad is fine ;)
            #-----------------------------------------------------------------.
            # "ISatSS": {
            #     # TODO 
            #     # - end_time? Seems to be only the c-reation time     
            #     # At 0000--> HFD 
            #     # At 0002,4,7,9 --> HR3
            #     "OR_HFD-{spatial_res:3d}-B{bits_per_pixel:3d}-M1C{channel:2d}-T{tile_number:3d}_GH8_s{start_time:%Y%j%H%M%S}0_c{creation_time:%Y%j%H%M%S}0.nc"
            #     "OR_HR3-{spatial_res:3d}-B{bits_per_pixel:3d}-M1C{channel:2d}-T{tile_number:3d}_GH8_s{start_time:%Y%j%H%M%S}0_c{creation_time:%Y%j%H%M%S}0.nc"
            #     },
            
            # "NDMW": {
            #     # Before 2021 named: https://noaa-himawari8.s3.amazonaws.com/index.html#AHI-L2-FLDK-Winds/2021/03/03/0020/ 
            #     # Himawari8_AHI_2KM_FLDK_2021061_0020_20_WINDS_AMV_EN-07-CT.nc
            #     # Himawari8_AHI_2KM_FLDK_2021061_0020_20_WINDS_AMV_EN-08-CS.nc
            #     # Himawari8_AHI_500M_FLDK_2021061_0020_20_WINDS_AMV_EN-03-CT.nc
            #     "NDMW-AHI-C{channel:2d}CT_v1r0_h08_{start_time:%Y%m%d%H%M%S}_e {end_time:%Y%m%d%H%M%S}_c{production_time:%Y%m%d%H%M%S}.nc",
            #     "NDMW-AHI-C{channel:2d}CS_v1r0_h08_{start_time:%Y%m%d%H%M%S}_e {end_time:%Y%m%d%H%M%S}_c{production_time:%Y%m%d%H%M%S}.nc",
            #     },
            
            
            # "SST": {
            #     # Before 2021 named: 
            #     # - https://noaa-himawari8.s3.amazonaws.com/index.html#AHI-L2-FLDK-SST/2020/02/02/0100/
            #     # - https://noaa-himawari8.s3.amazonaws.com/index.html#AHI-L2-FLDK-SST/2021/01/01/0000/
            #     # 20200202010000-STAR-L2C_GHRSST-SSTsubskin-AHI_H08-ACSPO_V2.60-v02.0-fv01.0.nc
            #     # 20210201010000-STAR-L2P_GHRSST-SSTsubskin-AHI_H08-ACSPO_V2.71-v02.0-fv01.0.nc
            #     # 20200202010000-STAR-L3C_GHRSST-SSTsubskin-AHI_H08-ACSPO_V2.60-v02.0-fv01.0.nc
            #     # 20210201010000-STAR-L3C_GHRSST-SSTsubskin-AHI_H08-ACSPO_V2.71-v02.0-fv01.0.nc
            #     # 20191210154000-STAR-L3U_GHRSST-SSTsubskin-AHI_H08-ACSPO_V2.60-v02.0-fv01.0.nc
                
            #     #"<YYYYMMDDHH>0000-NCCF-L2P_GHRSST-SSTsubskin-AHI_H08-ACSPO_V2.80-v02.0-fv01.0.nc",
            #     "{start_time:%Y%M%d%H}0000-NCCF-L2P_GHRSST-SSTsubskin-AHI_H08-ACSPO_V2.80-v02.0-fv01.0.nc",
            #     #"<YYYYMMDDHH>0000-NCCF-L3C_GHRSST-SSTsubskin-AHI_H08-ACSPO_V2.80-v02.0-fv01.0.nc",
            #     #"{start_time:%Y%M%d%H}0000-NCCF-L3C_GHRSST-SSTsubskin-AHI_H08-ACSPO_V2.80-v02.0-fv01.0.nc",
            #     #"ACSPO_V2.80_H08_AHI_<YYYY>-<MM>-<DD>_<HHMM>-<HHMM>_<YYYYMMDD>.<ssssss>.nc" # where is start time and where is end time ?
            #     #"ACSPO_V2.80_H08_AHI_{start_time:%Y-%m-%d_%H%M}-{end_time:%H%M_%Y%m%d.%f}.nc"
            #     },
            
            },       
    },
}

