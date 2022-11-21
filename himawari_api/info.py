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

import os
import datetime
import numpy as np
from trollsift import Parser
from himawari_api.checks import _check_group_by_key, _check_time
from himawari_api.alias import (
    BUCKET_PROTOCOLS,
    _satellites, 
    _sectors,
    _channels,
)



####--------------------------------------------------------------------------.
#### Dictionary retrievals


def get_dict_info_products(product_levels=None):                  
    """Return a dictionary with sensors, product_level and product informations.

    The dictionary has structure {sensor: {product_level: [products]}}
    Specifying `sensors` and/or `product_levels` allows to retrieve only
    specific portions of the dictionary.
    """
    from himawari_api.listing import PRODUCTS
    from himawari_api.checks import _check_product_levels
    
    sensor = "AHI"
    if product_levels is None:
        product_levels = available_product_levels()
    # Check product_levels
    product_levels = _check_product_levels(product_levels)
    # Subset by sensors
    sensor_dictionary = {sensor: PRODUCTS[sensor]}
    # Subset the product dictionary 
    listing_dict = {}
    for sensor, product_level_dict in sensor_dictionary.items():
        for product_level, products_dict in product_level_dict.items():
            if product_level in product_levels:
                if listing_dict.get(sensor) is None:
                    listing_dict[sensor] = {}
                listing_dict[sensor][product_level] = products_dict
    # Return filtered listing_dict
    return listing_dict


def get_dict_product_sensor(product_levels=None):
    """Return a dictionary with available product and corresponding sensors.

    The dictionary has structure {product: sensor}.
    Specifying `sensors` and/or `product_levels` allows to retrieve only a
    specific subset of the dictionary.
    """
    # Get product listing dictionary
    products_listing_dict = get_dict_info_products(product_levels=product_levels)
    # Retrieve dictionary
    products_sensor_dict = {}
    for sensor, product_level_dict in products_listing_dict.items():
        for product_level, products_dict in product_level_dict.items():
            for product in products_dict.keys():
                products_sensor_dict[product] = "AHI"
    return products_sensor_dict


def get_dict_sensor_products(sensors=None, product_levels=None):
    """Return a dictionary with available sensors and corresponding products.

    The dictionary has structure {sensor: [products]}.
    Specifying `sensors` and/or `product_levels` allows to retrieve only a
    specific subset of the dictionary.
    """
    products_sensor_dict = get_dict_product_sensor(
        sensors=sensors, product_levels=product_levels
    )
    sensor_product_dict = {}
    for k in set(products_sensor_dict.values()):
        sensor_product_dict[k] = []
    for product, sensor in products_sensor_dict.items():
        sensor_product_dict[sensor].append(product)
    return sensor_product_dict


def get_dict_product_product_level(sensors=None, product_levels=None):
    """Return a dictionary with available products and corresponding product_level.

    The dictionary has structure {product: product_level}.
    Specifying `sensors` and/or `product_levels` allows to retrieve only a
    specific subset of the dictionary.
    """
    # Get product listing dictionary
    products_listing_dict = get_dict_info_products(
        sensors=sensors, product_levels=product_levels
    )
    # Retrieve dictionary
    products_product_level_dict = {}
    for sensor, product_level_dict in products_listing_dict.items():
        for product_level, products_dict in product_level_dict.items():
            for product in products_dict.keys():
                products_product_level_dict[product] = product_level
    return products_product_level_dict


def get_dict_product_level_products(sensors=None, product_levels=None):
    """Return a dictionary with available product_levels and corresponding products.

    The dictionary has structure {product_level: [products]}.
    Specifying `sensors` and/or `product_levels` allows to retrieve only a
    specific subset of the dictionary.
    """
    products_product_level_dict = get_dict_product_product_level(
        sensors=sensors, product_levels=product_levels
    )
    product_level_product_dict = {}
    for k in set(products_product_level_dict.values()):
        product_level_product_dict[k] = []
    for product, sensor in products_product_level_dict.items():
        product_level_product_dict[sensor].append(product)
    return product_level_product_dict

#### -------------------------------------------------------------------------.
#### Availability 

def available_protocols():
    """Return a list of available cloud bucket protocols."""
    return BUCKET_PROTOCOLS


def available_satellites():
    """Return a list of available satellites."""
    return list(_satellites.keys())                                            


def available_sectors(product=None):
    """Return a list of available sectors.

    If `product` is specified, it returns the sectors available for such specific
    product.
    """
    from himawari_api.checks import _check_product
    from himawari_api.listing import AHI_L2_SECTOR_EXCEPTIONS

    sectors_keys = list(_sectors.keys())
    if product is None:
        return sectors_keys
    else:
        product = _check_product(product)
        specific_sectors = AHI_L2_SECTOR_EXCEPTIONS.get(product)
        if specific_sectors is None:
            return sectors_keys
        else:
            return specific_sectors


def available_product_levels():
    """Return a list of available product levels."""
    from himawari_api.listing import PRODUCTS
    product_levels = list(PRODUCTS["AHI"])        
    product_levels = np.unique(product_levels).tolist()
    return product_levels


def available_channels():
    """Return a list of available AHI channels."""
    channels = list(_channels.keys())
    return channels


def available_products(product_levels=None):
    """Return a list of available products.
    
    Specifying `product_levels` allows to retrieve only a specific subset of the list.
    """
    # Get product listing dictionary
    products_dict = get_dict_product_sensor(product_levels=product_levels)
    products = list(products_dict.keys())
    return products


def available_group_keys():
    """Return a list of available group_keys."""
    group_keys = [
        "product",
        "scene_abbr",  # ["R1","R2","R3","R4", "R5"]
        "channel",     # B**
        "sector",      # FLDK, Japan, Target, Landmark
        "platform_shortname",   
        "start_time",
        "end_time",
        "production_time",
        "spatial_res", 
        "segment_number",
        "segment_total"
    ]
    return group_keys


def available_connection_types():
    """Return a list of available connect_type to connect to cloud buckets."""
    return ["bucket", "https", "nc_bytes"]


#### -------------------------------------------------------------------------.
#### Information extraction from filepaths 


def infer_satellite_from_path(path): 
    """Infer the satellite from the file path."""
    himawari8_patterns = ['himawari8', 'himawari-8', 'H8', 'H08']
    himawari9_patterns = ['himawari9', 'himawari-9', 'H9', 'H09'] 
    if np.any([pattern in path for pattern in himawari8_patterns]):
        return 'himawari-8'
    if np.any([pattern in path for pattern in himawari9_patterns]):
        return 'himawari-9'
    else:
        raise ValueError("Unexpected HIMAWARI file path.")


def _infer_product_level(fpath):
    """Infer product_level from filepath."""
    fname = os.path.basename(fpath)
    # Check if it is a L2 product 
    l2_products = ["HYDRO_RAIN_RATE", "RRQPE", "CLOUD_HEIGHT", "CHGT", "CLOUD_MASK", "CMSK", "CLOUD_PHASE", "CPHS"]
    bool_valid_product = [product in fname for product in l2_products]
    if np.any(bool_valid_product):
        product_level = "L2"
    # Otherwise check if it is a L1b Rad product  
    # - It could also check that "_B" is in fname  
    elif 'HS' in fname:
        product_level = 'L1b'
    else: 
        raise ValueError(f"`product_level` could not be inferred from {fname}.")
    # Return product 
    return product_level 
    

def _infer_product(fpath):
    '''Infer product from filepath.'''
    fname = os.path.basename(fpath)
    # Check if it is a L2 product 
    l2_products = ["HYDRO_RAIN_RATE", "RRQPE", "CLOUD_HEIGHT", "CHGT", "CLOUD_MASK", "CMSK", "CLOUD_PHASE", "CPHS"]
    bool_valid_product = [product in fname for product in l2_products]
    if np.any(bool_valid_product):
        product = l2_products[np.argwhere(bool_valid_product)[0][0]]
    # Otherwise check if it is a L1b Rad product  
    # - It could also check that "_B" is in fname  
    elif 'HS' in fname:
        product = 'Rad'
    else: 
        raise ValueError(f"`product` could not be inferred from {fname}.")
    # Return product 
    return product 


def _infer_satellite(fpath):
    """Infer satellite from filepath."""
    fname = os.path.basename(fpath)
    # GG SUGGESTION: if [h08, himawari8, himawari-8] in fpath.lower()
    himawari8_patterns = ['himawari8', 'himawari-8', 'H8', 'H08']
    himawari9_patterns = ['himawari9', 'himawari-9', 'H9', 'H09'] 
    
    if np.any([pattern in fpath for pattern in himawari8_patterns]):
        return 'himawari-8'
    if np.any([pattern in fpath for pattern in himawari9_patterns]):
        return 'himawari-9'
    else: 
        raise ValueError(f"`satellite` could not be inferred from {fname}.")


def _separate_sector_observation_number(sector_observation_number):
    """Return (sector, scene_abbr, observation_number) from <sector><observation_number> string."""
    # See Table 4 in https://www.data.jma.go.jp/mscweb/en/himawari89/space_segment/hsd_sample/HS_D_users_guide_en_v12.pdf
    # - FLDK
    # - JP[01-04]  # 2.5 min ...  (4 times in 10 min)    # Japan (R1 and R2)
    # - R[301-304] # 2.5 min      (4 times in 10 min)    # Target Area  (R3)
    # - R[401-420] # 30 secs      (20 times in 10 min)   # LandMark Area (R4)
    # - R[501-520] # 30 secs      (20 times in 10 min)   # LandMark Area (R5)
    if "FLDK" in sector_observation_number:
        sector = "FLDK"
        scene_abbr = "F"
        observation_number = None
    elif "JP" in sector_observation_number:
        sector = "JP"
        scene_abbr = ["R1","R2"]
        observation_number = int(sector_observation_number[-2:])
    elif "R" in sector_observation_number:  
        region_index = int(sector_observation_number[1])
        if region_index == 3: 
            sector = "Target" 
            scene_abbr = "R3"
        elif region_index == 4: 
            sector = "Landmark"  
            scene_abbr = "R4"
        else: 
            sector = "Landmark"  
            scene_abbr = "R5"
        observation_number = int(sector_observation_number[2:])
    else:
        raise NotImplementedError("Adapt the file patterns.")
    return sector, scene_abbr, observation_number


def _get_info_from_filename(fname):
    """Retrieve file information dictionary from filename."""
    from himawari_api.listing import GLOB_FNAME_PATTERN
    
    # Infer sensor and product_level
    sensor = "AHI"
    product_level = _infer_product_level(fname)
    product = _infer_product(fname)
    
    # Retrieve file pattern
    fpattern = GLOB_FNAME_PATTERN[sensor][product_level][product]       
    
    # Retrieve information from filename 
    p = Parser(fpattern)
    info_dict = p.parse(fname)
    
    info_dict["sensor"] = sensor
    info_dict["product_level"] = product_level
        
    # Round start_time and end_time to minute resolution
    for time in ["start_time", "end_time", "production_time", "creation_time"]:
        try:
            info_dict[time] = _check_time(info_dict[time])
        except:
            None
    
    # Parse sector_observation_number in L1b Rad files
    # - FLDK
    # - JP[01-04]  # 2.5 min ...  (4 times in 10 min)    # Japan (R1 and R2)
    # - R[301-304] # 2.5 min      (4 times in 10 min)    # Target Area   (R3)
    # - R[401-420] # 30 secs      (20 times in 10 min)   # LandMark Area (R4)
    # - R[501-520] # 30 secs      (20 times in 10 min)   # LandMark Area (R5)
    sector_observation_number = info_dict.get("sector_observation_number", None) 
    if sector_observation_number is not None: 
        sector, scene_abbr, observation_number = _separate_sector_observation_number(sector_observation_number)
        info_dict["sector"] = sector 
        info_dict["scene_abbr"] = scene_abbr             
        if sector == "Japan" or (sector == "Target" and scene_abbr == "R3"): 
            start_time = info_dict["start_time"]
            start_time = start_time + (observation_number-1)*datetime.timedelta(minutes=2, seconds=30)
            end_time = start_time + datetime.timedelta(minutes=2, seconds=30)
            info_dict["start_time"] = start_time
            info_dict["end_time"] = end_time
        if sector == "Landmark" and scene_abbr in ["R4", "R5"]:
            start_time = info_dict["start_time"]
            start_time = start_time + (observation_number-1)*datetime.timedelta(seconds=30)
            end_time = start_time + datetime.timedelta(seconds=30)
            info_dict["start_time"] = start_time
            info_dict["end_time"] = end_time
            
    # Retrieve end_time if not available in the file name 
    # --> L2 before the change in 2021, and L1b Rad for FLDK
    end_time = info_dict.get('end_time', None)
    if end_time is None: 
        end_time = info_dict['start_time'] + datetime.timedelta(minutes=10)
        info_dict["end_time"] = end_time
        
    # Retrieve sector if not available (i.e in L2)
    sector = info_dict.get('sector', None)
    if sector is None: 
        sector = "FLDK" # must be a L2 Product (the fname does not contain sector info).
        info_dict["sector"] = sector
        
    # Special treatment to homogenize L2 product names of CLOUDS AND RRQPE 
    if info_dict['product_level'] == "L2":
        product = info_dict['product']
        if product in ["CLOUD_MASK", "CMSK"]:
            product = "CMSK"
        elif product in ["CPHS", "CLOUD_PHASE"]: 
            product = "CPHS"
        elif product in ["CHGT", "CLOUD_HEIGHT"]: 
            product = "CHGT"
        elif product in ["RRQPE", "HYDRO_RAIN_RATE"]: 
            product = "RRQPE"
        else: 
            raise NotImplementedError()
    info_dict["product"] = product
    
    # Derive satellite name from platform_shortname if available 
    # - {platform_fullname} = "Himawari8", "Himawari9"
    # - {platform_shortname} = "h08", "H08", "h09", "H09"
    platform_shortname = info_dict.get("platform_shortname", None) 
    platform_fullname = info_dict.get("platform_fullname", None) 
    if platform_shortname is not None: 
        if 'H08' == platform_shortname.upper():
            satellite = 'HIMAWARI-8'
        elif 'H09' == platform_shortname.upper():
            satellite = 'HIMAWARI-9'
        else:
            raise ValueError(f"Processing of satellite {platform_shortname} not yet implemented.")
    elif platform_fullname is not None: 
        if 'HIMAWARI8' == platform_fullname.upper():
            satellite = 'HIMAWARI-8'
        elif 'HIMAWARI9' == platform_fullname.upper():
            satellite = 'HIMAWARI-9'
        else:
            raise ValueError(f"Processing of satellite {platform_fullname} not yet implemented.")        
    else: 
        raise ValueError("Satellite name not derivable from file name.") 
    info_dict["satellite"] =  satellite  
        
    # Return info dictionary
    return info_dict


def _get_info_from_filepath(fpath):
    """Retrieve file information dictionary from filepath."""
    if not isinstance(fpath, str):
        raise TypeError("'fpath' must be a string.")
    fname = os.path.basename(fpath)
    return _get_info_from_filename(fname)


def _get_key_from_filepaths(fpaths, key):
    """Extract specific key information from a list of filepaths."""
    if isinstance(fpaths, str):
        fpaths = [fpaths]
    return [
        _get_info_from_filepath(fpath)[key] for fpath in fpaths
    ]


def get_key_from_filepaths(fpaths, key):
    """Extract specific key information from a list of filepaths."""
    if isinstance(fpaths, dict):
        fpaths = {k: _get_key_from_filepaths(v, key=key) for k, v in fpaths.items()}
    else:
        fpaths = _get_key_from_filepaths(fpaths, key=key)
    return fpaths 

####--------------------------------------------------------------------------.
#### Group filepaths 

def _group_fpaths_by_key(fpaths, product_level=None, key="start_time"):
    """Utils function to group filepaths by key contained into filename.""" 
    # - Retrieve key sorting index 
    list_key_values = [_get_info_from_filepath(fpath)[key] for fpath in fpaths]
    idx_key_sorting = np.array(list_key_values).argsort()
    # - Sort fpaths and key_values by key values
    fpaths = np.array(fpaths)[idx_key_sorting]
    list_key_values = np.array(list_key_values)[idx_key_sorting]
    # - Retrieve first occurence of new key value
    unique_key_values, cut_idx = np.unique(list_key_values, return_index=True)
    # - Split by key value
    fpaths_grouped = np.split(fpaths, cut_idx)[1:]
    # - Convert array of fpaths into list of fpaths 
    fpaths_grouped = [arr.tolist() for arr in fpaths_grouped]
    # - Create (key: files) dictionary
    fpaths_dict = dict(zip(unique_key_values, fpaths_grouped))
    return fpaths_dict


def group_files(fpaths, key="start_time"):
    """
    Group filepaths by key contained into filenames.

    Parameters
    ----------
    fpaths : list
        List of filepaths.
    key : str
        Key by which to group the list of filepaths.
        The default key is "start_time".
        See `himawari_api.available_group_keys()` for available grouping keys.

    Returns
    -------
    fpaths_dict : dict
        Dictionary with structure {<key>: list_fpaths_with_<key>}.

    """
    if isinstance(fpaths, dict): 
        raise TypeError("It's not possible to group a dictionary ! Pass a list of filepaths instead.")
    key = _check_group_by_key(key)
    fpaths_dict = _group_fpaths_by_key(fpaths=fpaths, key=key)
    return fpaths_dict
