#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Ghiggi Gionata

# goes_api is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# goes_api is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# goes_api. If not, see <http://www.gnu.org/licenses/>.

AHI_L1_PRODUCTS = {
    "Rad": "Radiances",
}

# TODO: TO ADAPT THIS FOR HIMAWARI PRODUCTS
AHI_L2_PRODUCTS = {
    "ACHA": "Cloud Top Height",
    "ACHT": "Cloud Top Temperature",
    "ACM": "Clear Sky Masks",
    "ACTP": "Cloud Top Phase",
    "ADP": "Aerosol Detection",
    "AICE": "Ice Concentration and Extent",  # only F
    "AITA": "Ice Thickness and Age",  # only F
    "AOD": "Aerosol Optical Depth ",  # only F, C
    "BRF": "Land Surface Bidirectional Reflectance Factor",
    "CMIP": "Cloud and Moisture Imagery",
    "COD": "Cloud Optical Depth",  # only F, C
    "CPS": "Cloud Particle Size",
    "CTP": "Cloud Top Pressure",  # only F, C
    "DMW": "Derived Motion Winds",
    "DMWV": "Derived Motion Winds (Clear Sky)",
    "DSI": "Derived Stability Indices", 
    "DSR": "Downward Shortwave Radiation",
    "FDC": "Fire/Hot Spot Characterization",
    "LSA": "Land Surface Albedo",
    "LST": "Land Surface Temperature",
    "LST2KM": "Land Surface Temperature",  # only F
    "LVMP": "Legacy Vertical Moisture Profile",
    "LVTP": "Legacy Vertical Temperature Profile",
    "MCMIP": "Cloud and Moisture Imagery (Multi-band format)",
    "RRQPE": "Rainfall Rate (QPE)",  # only F
    "RSR": "Reflected Shortwave Radiation at TOA",  # only F and C
    "SST": "Sea Surface (Skin) Temperature",  # only F
    "TPW": "Total Precipitable Water",
    "VAA": "Volcanic Ash ",  # only F
}

# TODO: TO ADAPT THIS FOR HIMAWARI PRODUCTS
AHI_L2_SECTOR_EXCEPTIONS = {
    "AICE": ["F"],
    "ACHT": ["F", "M"],
    "AITA": ["F"],
    "LST2KM": ["F"],
    "RRQPE": ["F"],
    "SST": ["F"],
    "VAA": ["F"],
    "AOD": ["F", "C"],
    "COD": ["F", "C"],
    "CTP": ["F", "C"],
    "RSR": ["F", "C"],
}

 
PRODUCTS = {
    "AHI": {
        "L1b": AHI_L1_PRODUCTS,
        "L2": AHI_L2_PRODUCTS,
    },
}

# TODO: TO ADAPT THIS FOR HIMAWARI
# --> See https://github.com/pytroll/satpy/blob/main/satpy/etc/readers/ahi_hsd.yaml#L290 

GLOB_FNAME_PATTERN = {
    "AHI": {
        "L1b": "{system_environment:2s}_{sensor:3s}-{product_level:s}-{product:3s}{scene_abbr:s}-{scan_mode:2s}{channel:3s}_{platform_shortname:3s}_s{start_time:%Y%j%H%M%S%f}_e{end_time:%Y%j%H%M%S%f}_c{creation_time:%Y%j%H%M%S%f}.nc{nc_version}",
        # ABI_L1B_pattern = '{system_environment:2s}_{sensor:3s}-L1b-{observation_type:3s}{scene_abbr:s}-{scan_mode:2s}C01_{platform_shortname:3s}_s{start_time:%Y%j%H%M%S%f}_e{end_time:%Y%j%H%M%S%f}_c{creation_time:%Y%j%H%M%S%f}_{suffix}.nc{nc_version}'
        "L2": "{system_environment:2s}_{sensor:3s}-{product_level:s}-{product_scene_abbr}-{scan_mode:s}_{platform_shortname:3s}_s{start_time:%Y%j%H%M%S%f}_e{end_time:%Y%j%H%M%S%f}_c{creation_time:%Y%j%H%M%S%f}.nc",
    },
}


# https://github.com/pytroll/satpy/blob/main/satpy/etc/readers/abi_l1b.yaml
# https://github.com/pytroll/satpy/blob/main/satpy/etc/readers/abi_l2_nc.yaml
