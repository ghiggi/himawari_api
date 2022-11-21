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
"""Define functions checking himawari_api inputs."""

import os
import datetime
import numpy as np
from himawari_api.alias import PROTOCOLS, _satellites, _sectors, _channels


def _check_protocol(protocol):
    """Check protocol validity."""
    if protocol is not None:
        if not isinstance(protocol, str):
            raise TypeError("`protocol` must be a string.")
        if protocol not in PROTOCOLS:
            raise ValueError(f"Valid `protocol` are {PROTOCOLS}.")
        if protocol == "local":
            protocol = "file"  # for fsspec LocalFS compatibility
    return protocol


def _check_base_dir(base_dir):
    """Check base_dir validity."""
    if base_dir is not None:
        if not isinstance(base_dir, str):
            raise TypeError("`base_dir` must be a string.")
        if not os.path.exists(base_dir):
            raise OSError(f"`base_dir` {base_dir} does not exist.")
        if not os.path.isdir(base_dir):
            raise OSError(f"`base_dir` {base_dir} is not a directory.")
    return base_dir


def _check_satellite(satellite):
    """Check satellite validity."""
    if not isinstance(satellite, str):
        raise TypeError("`satellite` must be a string.")
    # Retrieve satellite key accounting for possible aliases
    satellite_key = None
    for key, possible_values in _satellites.items():
        if satellite.upper() in possible_values:
            satellite_key = key
            break
    if satellite_key is None:
        valid_satellite_key = list(_satellites.keys())
        raise ValueError(f"Available satellite: {valid_satellite_key}")
    return satellite_key


def _check_sector(sector, product=None):                                      
    """Check sector validity."""
    from himawari_api.info import available_sectors 
    
    if sector is None: 
        raise ValueError("'sector' must be specified.")

    if not isinstance(sector, str):
        raise TypeError("`sector` must be a string.")
    # Retrieve sector key accounting for possible aliases
    sector_key = None
    for key, possible_values in _sectors.items():
        if sector.upper() in possible_values:
            sector_key = key
            break
    # Raise error if provided unvalid sector key
    if sector_key is None:
        valid_sector_keys = list(_sectors.keys())
        raise ValueError(f"Available sectors: {valid_sector_keys}")
    # Check the sector is valid for a given product (if specified)
    valid_sectors = available_sectors(product=product)
    if product is not None:
        if sector_key not in valid_sectors:
            raise ValueError(
                f"Valid sectors for product {product} are {valid_sectors}."
            )
    return sector_key


def _check_product_level(product_level, product=None):
    """Check product_level validity."""
    from himawari_api.info import get_dict_product_level_products
    
    if not isinstance(product_level, str):
        raise TypeError("`product_level` must be a string.")
    product_level = product_level.capitalize()
    if product_level not in ["L1b", "L2"]:
        raise ValueError("Available product levels are ['L1b', 'L2'].")
    if product is not None:
        if product not in get_dict_product_level_products()[product_level]:
            raise ValueError(
                f"`product_level` '{product_level}' does not include product '{product}'."
            )
    return product_level


def _check_product_levels(product_levels):
    """Check product_levels validity."""
    if isinstance(product_levels, str):
        product_levels = [product_levels]
    product_levels = [
        _check_product_level(product_level) for product_level in product_levels
    ]
    return product_levels

def _check_product(product, product_level=None):
    """Check product validity."""
    from himawari_api.info import available_products 
    
    if not isinstance(product, str):
        raise TypeError("`product` must be a string.")
    valid_products = available_products(product_levels=product_level)
    # Retrieve product by accounting for possible aliases (upper/lower case)
    product_key = None
    for possible_values in valid_products:
        if possible_values.upper() == product.upper():
            product_key = possible_values
            break
    if product_key is None:
        if product_level is None:
            raise ValueError(f"Available products: {valid_products}")
        else:
            product_level = "" if product_level is None else product_level
            raise ValueError(f"Available {product_level} products: {valid_products}")
    return product_key


def _check_time(time):
    """Check time validity."""
    if not isinstance(time, (datetime.datetime, datetime.date, np.datetime64, str)):
        raise TypeError(
            "Specify time with datetime.datetime objects or a "
            "string of format 'YYYY-MM-DD hh:mm:ss'."
        )
    # If np.datetime, convert to datetime.datetime
    if isinstance(time, np.datetime64):
        time = time.astype('datetime64[s]').tolist()
    # If datetime.date, convert to datetime.datetime
    if not isinstance(time, (datetime.datetime, str)):
        time = datetime.datetime(time.year, time.month, time.day, 0, 0, 0)
    if isinstance(time, str):
        try:
            time = datetime.datetime.fromisoformat(time)
        except ValueError:
            raise ValueError("The time string must have format 'YYYY-MM-DD hh:mm:ss'")
    
    # Set resolution to seconds
    time = time.replace(microsecond=0)
 
    # Round seconds to 00 / 30
    time = _correct_time_seconds(time)

    return time


def _correct_time_seconds(time):
    """Round datetime seconds to 00 or 30
    
    If [0-15] --> 00 
    If [15-45] --> 30 
    If [45-59] --> 0 (and add 1 minute)
    """
    if time.second > 45:
        time = time.replace(second = 0) 
        time = time + datetime.timedelta(minutes=1)
    elif time.second > 15 and time.second < 45:
        time = time.replace(second = 30) 
    elif time.second < 15:
        time = time.replace(second = 0) 
    return time


def _check_start_end_time(start_time, end_time):
    """Check start_time and end_time validity."""
    # Format input
    start_time = _check_time(start_time)
    end_time = _check_time(end_time)
    
    # Check start_time and end_time are chronological
    if start_time > end_time:
        raise ValueError("Provide start_time occuring before of end_time")
        
    # Check start_time and end_time are in the past
    if start_time > datetime.datetime.utcnow():
        raise ValueError("Provide a start_time occuring in the past.")
    if end_time > datetime.datetime.utcnow():
        raise ValueError("Provide a end_time occuring in the past.")
    return (start_time, end_time)


def _check_channel(channel):
    """Check channel validity."""
    if not isinstance(channel, str):
        raise TypeError("`channel` must be a string.")
    # Check channel follow standard name
    channel = channel.upper()
    if channel in list(_channels.keys()):
        return channel
    # Retrieve channel key accounting for possible aliases
    else:
        channel_key = None
        for key, possible_values in _channels.items():
            if channel.upper() in possible_values:
                channel_key = key
                break
        if channel_key is None:
            valid_channels_key = list(_channels.keys())
            raise ValueError(f"Available channels: {valid_channels_key}")
        return channel_key


def _check_channels(channels=None):
    """Check channels validity."""
    if channels is None:
        return channels
    if isinstance(channels, str):
        channels = [channels]
    channels = [_check_channel(channel) for channel in channels]
    return channels


def _check_scene_abbr(scene_abbr, sector=None):                 
    """Check AHI Japan, Target and Landmark sector scene_abbr validity."""
    if scene_abbr is None:
        return scene_abbr
    if sector is not None:
        if sector == "FLDK": 
            raise ValueError("`scene_abbr` must be specified only for Japan and Target sectors !")
    if not isinstance(scene_abbr, (str, list)):
        raise TypeError("Specify `scene_abbr` as string or list.")
    if isinstance(scene_abbr, str):
        scene_abbr = [scene_abbr]
    valid_scene_abbr = ["R1", "R2", "R3", "R4", "R5"]
    if not np.all(np.isin(scene_abbr, valid_scene_abbr)):
        raise ValueError(f"Valid `scene_abbr` values are {valid_scene_abbr}.")
    if sector is not None:
        if sector == "Japan": 
            valid_scene_abbr = ["R1", "R2"]
            if not np.all(np.isin(scene_abbr, valid_scene_abbr)):
                raise ValueError(f"Valid `scene_abbr` for Japan sector are {valid_scene_abbr}.")
        if sector == "Target":
            valid_scene_abbr = ["R3"]
            not np.all(np.isin(scene_abbr, valid_scene_abbr))
            raise ValueError(f"Valid `scene_abbr` for Target sector are {valid_scene_abbr}.")
        if sector == "Landmark":
            valid_scene_abbr = ["R4", "R5"]
            not np.all(np.isin(scene_abbr, valid_scene_abbr))
            raise ValueError(f"Valid `scene_abbr` for Landmark sector are {valid_scene_abbr}.")
            
    return scene_abbr


def _check_filter_parameters(filter_parameters, sector):          
    """Check filter parameters validity.

    It ensures that channels and scene_abbr are valid lists (or None).
    """
    if not isinstance(filter_parameters, dict):
        raise TypeError("filter_parameters must be a dictionary.")
    channels = filter_parameters.get("channels")
    scene_abbr = filter_parameters.get("scene_abbr")
    if channels:
        filter_parameters["channels"] = _check_channels(channels)
    if scene_abbr:
        filter_parameters["scene_abbr"] = _check_scene_abbr(scene_abbr, sector=sector)
    return filter_parameters


def _check_group_by_key(group_by_key):
    """Check group_by_key validity."""
    from himawari_api.info import available_group_keys
    
    if not isinstance(group_by_key, (str, type(None))):
        raise TypeError("`group_by_key`must be a string or None.")
    if group_by_key is not None:
        valid_group_by_key = available_group_keys()
        if group_by_key not in valid_group_by_key:
            raise ValueError(
                f"{group_by_key} is not a valid group_by_key. "
                f"Valid group_by_key are {valid_group_by_key}."
            )
    return group_by_key


def _check_connection_type(connection_type, protocol):
    """Check cloud bucket connection_type validity."""
    if not isinstance(connection_type, (str, type(None))):
        raise TypeError("`connection_type` must be a string (or None).")
    if protocol is None:
        connection_type = None
    if protocol in ["file", "local"]:
        connection_type = None  # set default
    if protocol in ["s3"]:
        # Set default connection type
        if connection_type is None:
            connection_type = "bucket"  # set default
        valid_connection_type = ["bucket", "https", "nc_bytes"]
        if connection_type not in valid_connection_type:
            raise ValueError(f"Valid `connection_type` are {valid_connection_type}.")
    return connection_type


def _check_interval_regularity(list_datetime):
    """Check regularity of a list of timesteps."""
    # TODO: raise info when missing between ... and ...
    if len(list_datetime) < 2:
        return None
    list_datetime = sorted(list_datetime)
    list_timedelta = np.diff(list_datetime)
    list_unique_timedelta = np.unique(list_timedelta)
    if len(list_unique_timedelta) != 1:
        raise ValueError("The time interval is not regular!")
