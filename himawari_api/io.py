#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Ghiggi Gionata

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
import fsspec
import datetime
import numpy as np
import pandas as pd
from trollsift import Parser
from .utils.time import _dt_to_year_doy_hour

####--------------------------------------------------------------------------.
#### Alias
 
_satellites = {
    "himawari-8": ["H8", "HIMAWARI-8", "HIMAWARI8"],
    "himawari-9": ["H9", "HIMAWARI-9", "HIMAWARI9"],
}

# TODO: Change C (and M?) based on himawari names ?
_sectors = {
    "C": ["CONUS", "PACUS", "C", "P"],
    "F": ["FULL", "FULLDISK", "FULL DISK", "F"],
    "M": ["MESOSCALE", "M1", "M2", "M"],
}

# TODO: Change BAND 3.. DIFFERENT FROM HIMAWARI AHI ... 
_channels = {
    "C01": ["C01", "1", "01", "0.47", "BLUE", "B"],
    "C02": ["C02", "2", "02", "0.64", "RED", "R"],
    "C03": ["C03", "3", "03", "0.86", "VEGGIE"],  # G
    "C04": ["C04", "4", "04", "1.37", "CIRRUS"],
    "C05": ["C05", "5", "05", "1.6", "SNOW/ICE"],
    "C06": ["C06", "6", "06", "2.2", "CLOUD PARTICLE SIZE", "CPS"],
    "C07": ["C07", "7", "07", "3.9", "IR SHORTWAVE WINDOW", "IR SHORTWAVE"],
    "C08": [
        "C08",
        "8",
        "08",
        "6.2",
        "UPPER-LEVEL TROPOSPHERIC WATER VAPOUR",
        "UPPER-LEVEL WATER VAPOUR",
    ],
    "C09": [
        "C09",
        "9",
        "09",
        "6.9",
        "MID-LEVEL TROPOSPHERIC WATER VAPOUR",
        "MID-LEVEL WATER VAPOUR",
    ],
    "C10": [
        "C10",
        "10",
        "10",
        "7.3",
        "LOWER-LEVEL TROPOSPHERIC WATER VAPOUR",
        "LOWER-LEVEL WATER VAPOUR",
    ],
    "C11": ["C11", "11", "11", "8.4", "CLOUD-TOP PHASE", "CTP"],
    "C12": ["C12", "12", "12", "9.6", "OZONE"],
    "C13": ["C13", "13", "10.3", "CLEAN IR LONGWAVE WINDOW", "CLEAN IR"],
    "C14": ["C14", "14", "11.2", "IR LONGWAVE WINDOW", "IR LONGWAVE"],
    "C15": ["C15", "15", "12.3", "DIRTY LONGWAVE WINDOW", "DIRTY IR"],
    "C16": ["C16", "16", "13.3", "CO2 IR LONGWAVE", "CO2", "CO2 IR"],
}

PROTOCOLS = ["s3", "local", "file"]
BUCKET_PROTOCOLS = ["s3"]

####--------------------------------------------------------------------------.
#### Availability


def available_protocols():
    """Return a list of available cloud bucket protocols."""
    return BUCKET_PROTOCOLS


def available_sensors():
    """Return a list of available sensors."""
    from himawari_api.listing import PRODUCTS

    return list(PRODUCTS.keys())


def available_satellites():
    """Return a list of available satellites."""
    return list(_satellites.keys())


def available_sectors(product=None):
    """Return a list of available sectors.

    If `product` is specified, it returns the sectors available for such specific
    product.
    """
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


def available_product_levels(sensors=None):
    """Return a list of available product levels.

    If `sensors` is specified, it returns the product levels available for
    the specified set of sensors.
    """

    from himawari_api.listing import PRODUCTS

    if sensors is None:
        return ["L1b", "L2"]
    else:
        if isinstance(sensors, str):
            sensors = [sensors]
        product_levels = np.concatenate(
            [list(PRODUCTS[sensor].keys()) for sensor in sensors]
        )
        product_levels = np.unique(product_levels).tolist()
        return product_levels


def available_scan_modes():
    """Return available scan modes for AHI.

    Scan modes:
    - M3: Default scan strategy before 2 April 2019.
          --> Full Disk every 15 minutes
          --> CONUS/PACUS every 5 minutes
          --> M1 and M2 every 1 minute
    - M6: Default scan strategy since 2 April 2019.
          --> Full Disk every 10 minutes
          --> CONUS/PACUS every 5 minutes
          --> M1 and M2 every 5 minutes
    - M4: Only Full Disk every 5 minutes
          --> Example: Dec 4 2018, ...
    """
    # TODO: UPDATE THE DOC BASED ON HIMAWARI SCAN MODES 
    scan_mode = ["M3", "M4", "M6"]
    return scan_mode


def available_channels():
    """Return a list of available AHI channels."""
    channels = list(_channels.keys())
    return channels


def available_products(sensors=None, product_levels=None):
    """Return a list of available products.

    Specifying `sensors` and/or `product_levels` allows to retrieve only a
    specific subset of the list.
    """
    # Get product listing dictionary
    products_dict = get_dict_product_sensor(
        sensors=sensors, product_levels=product_levels
    )
    products = list(products_dict.keys())
    return products


def available_group_keys():
    """Return a list of available group_keys."""
    # TODO: MAYBE TO ADAPT BASED ON THE GLOB FPATTERN ...
    group_keys = [
        "system_environment",
        "sensor",  # AHI
        "product_level",
        "product",       # ...
        "scene_abbr",  # ["F", "C", "M1", "M2"]
        "scan_mode",  # ["M3", "M4", "M6"]
        "channel",  # C**
        "platform_shortname",  # G16, G17
        "start_time",
        "end_time",
    ]
    return group_keys


def available_connection_types():
    """Return a list of available connect_type to connect to cloud buckets."""
    return ["bucket", "https", "nc_bytes"]


####--------------------------------------------------------------------------.
#### Checks


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


def _check_sector(sector, sensor, product=None):
    """Check sector validity."""
    if sector is None: 
        if sensor == "AHI":
            raise ValueError("If sensor='AHI', `sector` must be specified!")
        return sector 
    if sector is not None and sensor != "AHI": 
        raise ValueError("`sector`must be specified only for sensor='AHI'.")
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
        raise ValueError(f"Available satellite: {valid_sector_keys}")
    # Check the sector is valid for a given product (if specified)
    valid_sectors = available_sectors(product=product)
    if product is not None:
        if sector_key not in valid_sectors:
            raise ValueError(
                f"Valid sectors for product {product} are {valid_sectors}."
            )
    return sector_key


def _check_sensor(sensor):
    """Check sensor validity."""
    if not isinstance(sensor, str):
        raise TypeError("`sensor` must be a string.")
    valid_sensors = available_sensors()
    sensor = sensor.upper()
    if sensor not in valid_sensors:
        raise ValueError(f"Available sensors: {valid_sensors}")
    return sensor


def _check_sensors(sensors):
    """Check sensors validity."""
    if isinstance(sensors, str):
        sensors = [sensors]
    sensors = [_check_sensor(sensor) for sensor in sensors]
    return sensors


def _check_product_level(product_level, product=None):
    """Check product_level validity."""
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


def _check_product(product, sensor=None, product_level=None):
    """Check product validity."""
    if not isinstance(product, str):
        raise TypeError("`product` must be a string.")
    valid_products = available_products(sensors=sensor, product_levels=product_level)
    # Retrieve product by accounting for possible aliases (upper/lower case)
    product_key = None
    for possible_values in valid_products:
        if possible_values.upper() == product.upper():
            product_key = possible_values
            break
    if product_key is None:
        if sensor is None and product_level is None:
            raise ValueError(f"Available products: {valid_products}")
        else:
            sensor = "" if sensor is None else sensor
            product_level = "" if product_level is None else product_level
            raise ValueError(
                f"Available {product_level} products for {sensor}: {valid_products}"
            )
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
    return time


def _check_start_end_time(start_time, end_time):
    """Check start_time and end_time validity."""
    # Format input
    start_time = _check_time(start_time)
    end_time = _check_time(end_time)
    # Set resolution to minutes (TODO: CONSIDER POSSIBLE MESOSCALE AT 30 SECS)
    start_time = start_time.replace(microsecond=0, second=0)
    end_time = end_time.replace(microsecond=0, second=0)
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


def _check_channels(channels=None, sensor=None):
    """Check channels validity."""
    if channels is None:
        return channels
    if sensor is not None:
        if sensor != "AHI":
            raise ValueError("`sensor` must be 'AHI' if the channels are specified!")
    if isinstance(channels, str):
        channels = [channels]
    channels = [_check_channel(channel) for channel in channels]
    return channels


def _check_scan_mode(scan_mode):
    """Check scan_mode validity."""
    if not isinstance(scan_mode, str):
        raise TypeError("`scan_mode` must be a string.")
    # Check channel follow standard name
    scan_mode = scan_mode.upper()
    valid_scan_modes = available_scan_modes()
    if scan_mode in valid_scan_modes:
        return scan_mode
    else:
        raise ValueError(f"Available `scan_mode`: {valid_scan_modes}")


def _check_scan_modes(scan_modes=None, sensor=None):
    """Check scan_modes validity."""
    if scan_modes is None:
        return scan_modes
    if sensor is not None:
        if sensor != "AHI":
            raise ValueError("`sensor` must be 'AHI' if the scan_mode is specified!")
    if isinstance(scan_modes, str):
        scan_modes = [scan_modes]
    scan_modes = [_check_scan_mode(scan_mode) for scan_mode in scan_modes]
    return scan_modes


def _check_scene_abbr(scene_abbr, sensor=None, sector=None):
    """Check AHI mesoscale sector scene_abbr validity."""
    if scene_abbr is None:
        return scene_abbr
    if sensor is not None:
        if sensor != "AHI":
            raise ValueError("`sensor` must be 'AHI' if the scene_abbr is specified!")
    if sector is not None:
        if sector != "M":
            raise ValueError("`scene_abbr` must be specified only if sector=='M' !")
    if not isinstance(scene_abbr, (str, list)):
        raise TypeError("Specify `scene_abbr` as string or list.")
    if isinstance(scene_abbr, list):
        if len(scene_abbr) == 1:
            scene_abbr = scene_abbr[0]
        else:
            return None  # set to None assuming ['M1' and 'M2']
    if scene_abbr not in ["M1", "M2"]:
        raise ValueError("Valid `scene_abbr` values are 'M1' or 'M2'.")
    return scene_abbr


def _check_filter_parameters(filter_parameters, sensor, sector):
    """Check filter parameters validity.

    It ensures that scan_modes, channels and scene_abbr are valid lists (or None).
    """
    if not isinstance(filter_parameters, dict):
        raise TypeError("filter_parameters must be a dictionary.")
    scan_modes = filter_parameters.get("scan_modes")
    channels = filter_parameters.get("channels")
    scene_abbr = filter_parameters.get("scene_abbr")
    if scan_modes:
        filter_parameters["scan_modes"] = _check_scan_modes(scan_modes)
    if channels:
        filter_parameters["channels"] = _check_channels(channels, sensor=sensor)
    if scene_abbr:
        filter_parameters["scene_abbr"] = _check_scene_abbr(
            scene_abbr, sensor=sensor, sector=sector
        )
    return filter_parameters


def _check_group_by_key(group_by_key):
    """Check group_by_key validity."""
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


def _check_unique_scan_mode(fpath_dict, sensor, product_level):
    """Check files have unique scan_mode validity."""
    # TODO: raise information when it changes
    if sensor == "AHI":
        list_datetime = list(fpath_dict.keys())
        fpaths_examplars = [fpath_dict[tt][0] for tt in list_datetime]
        list_scan_modes = _get_key_from_filepaths(
            fpaths_examplars, key="scan_mode", sensor=sensor, product_level=product_level,
        )
        list_scan_modes = np.unique(list_scan_modes).tolist()
        if len(list_scan_modes) != 1:
            raise ValueError(
                f"There is a mixture of the following scan_mode: {list_scan_modes}."
            )


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


####--------------------------------------------------------------------------.
#### Dictionary retrievals


def get_available_online_product(protocol, satellite):
    """Get a dictionary of available products in a specific cloud bucket.

    The dictionary has structure {sensor: {product_level: [products]}}.

    Parameters
    ----------
    protocol : str
        String specifying the cloud bucket storage that you want to explore.
        Use `himawari_api.available_protocols()` to retrieve available protocols.
    satellite : str
        The name of the satellite.
        Use `himawari_api.available_satellites()` to retrieve the available satellites.
    """
    # Get filesystem and bucket
    fs = get_filesystem(protocol)
    bucket = get_bucket(protocol, satellite)
    # List contents of the satellite bucket.
    list_dir = fs.ls(bucket)
    list_dir = [path for path in list_dir if fs.isdir(path)]
    # Retrieve directories name
    list_dirname = [os.path.basename(f) for f in list_dir]
    # Remove sector letter for AHI folders
    list_products = [
        product[:-1] if product.startswith("AHI") else product
        for product in list_dirname
    ]
    list_products = np.unique(list_products).tolist()
    # Retrieve sensor, product_level and product list
    list_sensor_level_product = [product.split("-") for product in list_products]
    # Build a dictionary
    products_dict = {}
    for sensor, product_level, product in list_sensor_level_product:
        if products_dict.get(sensor) is None:
            products_dict[sensor] = {}
        if products_dict[sensor].get(product_level) is None:
            products_dict[sensor][product_level] = []
        products_dict[sensor][product_level].append(product)
    # Return dictionary
    return products_dict


def get_dict_info_products(sensors=None, product_levels=None):
    """Return a dictionary with sensors, product_level and product informations.

    The dictionary has structure {sensor: {product_level: [products]}}
    Specifying `sensors` and/or `product_levels` allows to retrieve only
    specific portions of the dictionary.
    """
    from himawari_api.listing import PRODUCTS

    if sensors is None and product_levels is None:
        return PRODUCTS
    if sensors is None:
        sensors = available_sensors()
    if product_levels is None:
        product_levels = available_product_levels()
    # Subset by sensors
    sensors = _check_sensors(sensors)
    intermediate_listing = {sensor: PRODUCTS[sensor] for sensor in sensors}
    # Subset by product_levels
    product_levels = _check_product_levels(product_levels)
    listing_dict = {}
    for sensor, product_level_dict in intermediate_listing.items():
        for product_level, products_dict in product_level_dict.items():
            if product_level in product_levels:
                if listing_dict.get(sensor) is None:
                    listing_dict[sensor] = {}
                listing_dict[sensor][product_level] = products_dict
    # Return filtered listing_dict
    return listing_dict


def get_dict_product_sensor(sensors=None, product_levels=None):
    """Return a dictionary with available product and corresponding sensors.

    The dictionary has structure {product: sensor}.
    Specifying `sensors` and/or `product_levels` allows to retrieve only a
    specific subset of the dictionary.
    """
    # Get product listing dictionary
    products_listing_dict = get_dict_info_products(
        sensors=sensors, product_levels=product_levels
    )
    # Retrieve dictionary
    products_sensor_dict = {}
    for sensor, product_level_dict in products_listing_dict.items():
        for product_level, products_dict in product_level_dict.items():
            for product in products_dict.keys():
                products_sensor_dict[product] = sensor
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


####--------------------------------------------------------------------------.
#### Filesystems, buckets and directory structures
def get_filesystem(protocol, fs_args={}):
    """
    Define ffspec filesystem.

    protocol : str
       String specifying the cloud bucket storage from which to retrieve
       the data. It must be specified if not searching data on local storage.
       Use `himawari_api.available_protocols()` to retrieve available protocols.
    fs_args : dict, optional
       Dictionary specifying optional settings to initiate the fsspec.filesystem.
       The default is an empty dictionary. Anonymous connection is set by default.

    """
    if not isinstance(fs_args, dict):
        raise TypeError("fs_args must be a dictionary.")
    if protocol == "s3":
        # Set defaults
        # - Use the anonymous credentials to access public data
        _ = fs_args.setdefault("anon", True)  # TODO: or if is empty
        fs = fsspec.filesystem("s3", **fs_args)
        return fs
    elif protocol in ["local", "file"]:
        fs = fsspec.filesystem("file")
        return fs
    else:
        raise NotImplementedError(
            "Current available protocols are 's3', 'local'."
        )


def get_bucket(protocol, satellite):
    """
    Get the cloud bucket address for a specific satellite.

    Parameters
    ----------
    protocol : str
         String specifying the cloud bucket storage from which to retrieve
         the data. Use `himawari_api.available_protocols()` to retrieve available protocols.
    satellite : str
        The acronym of the satellite.
        Use `himawari_api.available_satellites()` to retrieve the available satellites.
    """

    # Dictionary of bucket and urls
    bucket_dict = {
        "s3": "s3://noaa-{}".format(satellite.replace("-", "")), 
    }
    return bucket_dict[protocol]

def _switch_to_https_fpath(fpath, protocol): 
    """
    Switch bucket address with https address.

    Parameters
    ----------
    fpath : str
        A single bucket filepaths.
    protocol : str
         String specifying the cloud bucket storage from which to retrieve
         the data. Use `himawari_api.available_protocols()` to retrieve available protocols.
    """
    satellite = infer_satellite_from_path(fpath)
    https_base_url_dict = {
        "s3": "https://noaa-{}.s3.amazonaws.com".format(satellite.replace("-", "")),
    }
    base_url = https_base_url_dict[protocol]
    fpath = os.path.join(base_url, fpath.split("/", 3)[3])  
    return fpath 
    

def _switch_to_https_fpaths(fpaths, protocol):
    """
    Switch bucket address with https address.

    Parameters
    ----------
    fpaths : list
        List of bucket filepaths.
    protocol : str
         String specifying the cloud bucket storage from which to retrieve
         the data. Use `himawari_api.available_protocols()` to retrieve available protocols.
    """
    fpaths = [_switch_to_https_fpath(fpath, protocol) for fpath in fpaths]
    return fpaths


def _get_bucket_prefix(protocol):
    """Get protocol prefix."""
    if protocol == "s3":
        prefix = "s3://"
    elif protocol == "file":
        prefix = ""
    else:
        raise NotImplementedError(
            "Current available protocols are 's3', 'local'."
        )
    return prefix

# TODO: THIS MIGHT BE UPDATED 
def _get_product_name(sensor, product_level, product, sector):
    """Get bucket directory name of a product."""
    if sensor=='AHI':
        product_name = f"{sensor}-{product_level}-{product}{sector}"
    else: 
        product_name = f"{sensor}-{product_level}-{product}"
    return product_name

def _get_product_dir(
    satellite, sensor, product_level, product, sector, protocol=None, base_dir=None
):
    """Get product (bucket) directory path."""
    if base_dir is None:
        bucket = get_bucket(protocol, satellite)
    else:
        bucket = os.path.join(base_dir, satellite.upper())
        if not os.path.exists(bucket):
            raise OSError(f"The directory {bucket} does not exist.")
    product_name = _get_product_name(sensor, product_level, product, sector)
    product_dir = os.path.join(bucket, product_name)
    return product_dir


def infer_satellite_from_path(path): 
    """Infer the satellite from the file path."""
    himawari8_patterns = ['himawari8', 'himawari-8', 'H8']
    himawari9_patterns = ['himawari9', 'himawari-9', 'H9'] 
    if np.any([pattern in path for pattern in himawari8_patterns]):
        return 'himawari-16'
    if np.any([pattern in path for pattern in himawari9_patterns]):
        return 'himawari-17'
    else:
        raise ValueError("Unexpected HIMAWARI file path.")


def remove_bucket_address(fpath):
    """Remove the bucket acronym (i.e. s3://) from the file path."""
    fel = fpath.split("/")[3:]
    fpath = os.path.join(*fel)
    return fpath

####---------------------------------------------------------------------------.
#### Filtering
def _infer_product_level(fpath):
    """Infer product_level from filepath."""
    fname = os.path.basename(fpath)
    if '-L1b-' in fname: 
        return 'L1b'
    elif '-L2-' in fname: 
        return 'L2'
    else: 
        raise ValueError(f"`product_level` could not be inferred from {fname}.")

### TODO: THIS MUST LIKELY BE CHANGED
def _infer_sensor(fpath):
    """Infer sensor from filepath."""
    fname = os.path.basename(fpath)
    if '_AHI-' in fname: 
        return 'AHI'
    else: 
        raise ValueError(f"`sensor` could not be inferred from {fname}.")

# TODO HERE TO MODIFY FOR HIMAWARI ... H8? NOT SURE
def _infer_satellite(fpath):
    """Infer satellite from filepath."""
    fname = os.path.basename(fpath)
    if '_H8_' in fname: 
        return 'HIMAWARI-8'
    elif '_H9_-' in fname: 
        return 'HIMAWARI-9'
    else: 
        raise ValueError(f"`satellite` could not be inferred from {fname}.")

# TODO HERE TO MODIFY FOR HIMAWARI
def _separate_product_scene_abbr(product_scene_abbr):
    """Return (product, scene_abbr) from <product><scene_abbr> string."""
    last_letter = product_scene_abbr[-1]
    # Mesoscale domain
    if last_letter in ["1", "2"]:
        return product_scene_abbr[:-2], product_scene_abbr[-2:]
    # CONUS and Full Disc
    elif last_letter in ["F", "C"]:
        return product_scene_abbr[:-1], product_scene_abbr[-1]
    else:
        raise NotImplementedError("Adapat the file patterns.")


def _get_info_from_filename(fname, sensor=None, product_level=None):
    """Retrieve file information dictionary from filename."""
    # TODO: sensor and product_level can be removed as function arguments
    from himawari_api.listing import GLOB_FNAME_PATTERN
    # Infer sensor and product_level if not provided
    if sensor is None:
        sensor = _infer_sensor(fname)
    if product_level is None: 
        product_level = _infer_product_level(fname)
        
    # Retrieve file pattern
    fpattern = GLOB_FNAME_PATTERN[sensor][product_level]
    
    # Retrieve information from filename 
    p = Parser(fpattern)
    info_dict = p.parse(fname)
    
    # Assert sensor and product_level are correct
    assert sensor == info_dict['sensor']
    assert product_level == info_dict['product_level']
    
    # Round start_time and end_time to minute resolution
    info_dict["start_time"] = info_dict["start_time"].replace(microsecond=0, second=0)
    info_dict["end_time"] = info_dict["end_time"].replace(microsecond=0, second=0)
    
    # Special treatment for AHI L2 products
    if info_dict.get("product_scene_abbr") is not None:
        # Identify scene_abbr
        product, scene_abbr = _separate_product_scene_abbr(
            info_dict.get("product_scene_abbr")
        )
        info_dict["product"] = product
        info_dict["scene_abbr"] = scene_abbr
        del info_dict["product_scene_abbr"]
        # Special treatment for CMIP to extract channels
        if product == "CMIP":
            scan_mode_channels = info_dict["scan_mode"]
            scan_mode = scan_mode_channels[0:3]
            channels = scan_mode_channels[3:]
            info_dict["scan_mode"] = scan_mode
            info_dict["channels"] = channels
            
    # Special treatment for AHI products to retrieve sector 
    if sensor == 'AHI':
        if 'M' in info_dict["scene_abbr"]:
            sector = 'M'
        else: 
            sector = info_dict["scene_abbr"] 
        info_dict["sector"] =  sector   
    
    # Derive satellite name  
    platform_shortname = info_dict["platform_shortname"]
    if 'H8' == platform_shortname:
        satellite = 'HIMAWARI-8'
    elif 'H9' == platform_shortname:
        satellite = 'HIMAWARI-9'
    else:
        raise ValueError(f"Processing of satellite {platform_shortname} not yet implemented.")
    info_dict["satellite"] =  satellite  
        
    # Return info dictionary
    return info_dict


def _get_info_from_filepath(fpath, sensor=None, product_level=None):
    """Retrieve file information dictionary from filepath."""
    if not isinstance(fpath, str):
        raise TypeError("'fpath' must be a string.")
    fname = os.path.basename(fpath)
    return _get_info_from_filename(fname, sensor, product_level)


def _get_key_from_filepaths(fpaths, key, sensor=None, product_level=None):
    """Extract specific key information from a list of filepaths."""
    if isinstance(fpaths, str):
        fpaths = [fpaths]
    return [
        _get_info_from_filepath(fpath, sensor, product_level)[key] for fpath in fpaths
    ]


def get_key_from_filepaths(fpaths, key):
    """Extract specific key information from a list of filepaths."""
    if isinstance(fpaths, dict):
        fpaths = {k: _get_key_from_filepaths(v, key=key) for k, v in fpaths.items()}
    else:
        fpaths = _get_key_from_filepaths(fpaths, key=key)
    return fpaths 

def _filter_file(
    fpath,
    sensor,
    product_level,
    start_time=None,
    end_time=None,
    scan_modes=None,
    channels=None,
    scene_abbr=None,
):
    """Utility function to filter a filepath based on optional filter_parameters."""
    # scan_mode and channels must be list, start_time and end_time a datetime object

    # Get info from filepath
    info_dict = _get_info_from_filepath(fpath, sensor, product_level)

    # Filter by channels
    if channels is not None:
        file_channel = info_dict.get("channel")
        if file_channel is not None:
            if file_channel not in channels:
                return None

    # Filter by scan mode
    if scan_modes is not None:
        file_scan_mode = info_dict.get("scan_mode")
        if file_scan_mode is not None:
            if file_scan_mode not in scan_modes:
                return None

    # Filter by scene_abbr
    if scene_abbr is not None:
        file_scene_abbr = info_dict.get("scene_abbr")
        if file_scene_abbr is not None:
            if file_scene_abbr not in scene_abbr:
                return None
    
    # Filter by start_time
    if start_time is not None:
        # If the file ends before start_time, do not select
        # - Do not use <= because mesoscale data can have start_time=end_time at min resolution
        file_end_time = info_dict.get("end_time")
        if file_end_time < start_time: 
            return None
        # This would exclude a file with start_time within the file
        # if file_start_time < start_time:
        #     return None

    # Filter by end_time
    if end_time is not None:
        file_start_time = info_dict.get("start_time")
        # If the file starts after end_time, do not select
        if file_start_time > end_time:
            return None
        # This would exclude a file with end_time within the file
        # if file_end_time > end_time:
        #     return None
    return fpath


def _filter_files(
    fpaths,
    sensor,
    product_level,
    start_time=None,
    end_time=None,
    scan_modes=None,
    channels=None,
    scene_abbr=None,
):
    """Utility function to select filepaths matching optional filter_parameters."""
    if isinstance(fpaths, str):
        fpaths = [fpaths]
    fpaths = [
        _filter_file(
            fpath,
            sensor,
            product_level,
            start_time=start_time,
            end_time=end_time,
            scan_modes=scan_modes,
            channels=channels,
            scene_abbr=scene_abbr,
        )
        for fpath in fpaths
    ]
    fpaths = [fpath for fpath in fpaths if fpath is not None]
    return fpaths


def filter_files(
    fpaths,
    sensor,
    product_level,
    start_time=None,
    end_time=None,
    scan_modes=None,
    scene_abbr=None,
    channels=None,
):
    """
    Filter files by optional parameters.

    The optional parameters can also be defined within a `filter_parameters`
    dictionary which is then passed to `find_files` or `download_files` functions.

    Parameters
    ----------
    fpaths : list
        List of filepaths.
    sensor : str
        Satellite sensor.
        See `himawari_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `himawari_api.available_product_levels()` for available product levels.
    start_time : datetime.datetime, optional
        Time defining interval start.
        The default is None (no filtering by start_time).
    end_time : datetime.datetime, optional
        Time defining interval end.
        The default is None (no filtering by end_time).
    scan_modes : list, optional
        List of AHI scan modes to select.
        See `himawari_api.available_scan_modes()` for available scan modes.
        The default is None (no filtering by scan_modes).
    scene_abbr : str, optional
        String specifying selection of mesoscale scan region.
        Either M1 or M2.
        The default is None (no filtering by mesoscale scan region).
    channels : list, optional
        List of AHI channels to select.
        See `himawari_api.available_channels()` for available AHI channels.
        The default is None (no filtering by channels).

    """
    sensor = _check_sensor(sensor)
    product_level = _check_product_level(product_level, product=None)
    scan_modes = _check_scan_modes(scan_modes)
    channels = _check_channels(channels, sensor=sensor)
    scene_abbr = _check_scene_abbr(scene_abbr, sensor=sensor)
    start_time, end_time = _check_start_end_time(start_time, end_time)
    fpaths = _filter_files(
        fpaths=fpaths,
        sensor=sensor,
        product_level=product_level,
        start_time=start_time,
        end_time=end_time,
        scan_modes=scan_modes,
        channels=channels,
        scene_abbr=scene_abbr,
    )
    return fpaths


####---------------------------------------------------------------------------.
#### Search files
def _get_acquisition_max_timedelta(sector):
    """Get reasonable timedelta based on AHI sector to find previous/next acquisition."""
    if sector == "M":
        dt = datetime.timedelta(minutes=1)
    elif sector == "C":
        dt = datetime.timedelta(minutes=5)
    elif sector == "F":
        dt = datetime.timedelta(minutes=15)  # to include all scan_mode options
    else: # sector=None (all other sensors)  # TODO: might be improved ... 
        dt = datetime.timedelta(minutes=15)
    return dt


def _group_fpaths_by_key(fpaths, sensor=None, product_level=None, key="start_time"):
    """Utils function to group filepaths by key contained into filename."""
    # TODO: sensor and product_level args could be removed 
    # - Retrieve key sorting index 
    list_key_values = [
        _get_info_from_filepath(fpath, sensor, product_level)[key] for fpath in fpaths
    ]
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


def find_files(
    satellite,
    sensor,
    product_level,
    product,
    start_time,
    end_time,
    sector=None, 
    filter_parameters={},
    group_by_key=None,
    connection_type=None,
    base_dir=None,
    protocol=None,
    fs_args={},
    verbose=False,
):
    """
    Retrieve files from local or cloud bucket storage.

    If you are querying mesoscale domain data (sector=M), you might be
      interested to specify in the filter_parameters dictionary the
      key `scene_abbr` with values "M1" or "M2".

    Parameters
    ----------
    base_dir : str
        Base directory path where the <HIMAWARI-**> satellite is located.
        This argument must be specified only if searching files on local storage.
        If it is specified, protocol and fs_args arguments must not be specified.
    protocol : str
        String specifying the cloud bucket storage from which to retrieve
        the data. It must be specified if not searching data on local storage.
        Use `himawari_api.available_protocols()` to retrieve available protocols.
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    satellite : str
        The name of the satellite.
        Use `himawari_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `himawari_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `himawari_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `himawari_api.available_products()` for a list of available products.
    start_time : datetime.datetime
        The start (inclusive) time of the interval period for retrieving the filepaths.
    end_time : datetime.datetime
        The end (exclusive) time of the interval period for retrieving the filepaths.
    sector : str
        The acronym of the AHI sector for which to retrieve the files.
        See `himawari_api.available_sectors()` for a list of available sectors.
    filter_parameters : dict, optional
        Dictionary specifying option filtering parameters.
        Valid keys includes: `channels`, `scan_modes`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    group_by_key : str, optional
        Key by which to group the list of filepaths
        See `himawari_api.available_group_keys()` for available grouping keys.
        If a key is provided, the function returns a dictionary with grouped filepaths.
        By default, no key is specified and the function returns a list of filepaths.
    connection_type : str, optional
        The type of connection to a cloud bucket.
        This argument applies only if working with cloud buckets (base_dir is None).
        See `himawari_api.available_connection_types` for implemented solutions.
    verbose : bool, optional
        If True, it print some information concerning the file search.
        The default is False.

    """

    # Check inputs
    if protocol is None and base_dir is None:
        raise ValueError("Specify 1 between `base_dir` and `protocol`")
    if base_dir is not None:
        if protocol is not None:
            if protocol not in ["file", "local"]:
                raise ValueError("If base_dir is specified, protocol must be None.")
        else:
            protocol = "file"
            fs_args = {}
    # Format inputs
    protocol = _check_protocol(protocol)
    base_dir = _check_base_dir(base_dir)
    connection_type = _check_connection_type(connection_type, protocol)
    satellite = _check_satellite(satellite)
    sensor = _check_sensor(sensor)
    product_level = _check_product_level(product_level, product=None)
    product = _check_product(product, sensor=sensor, product_level=product_level)
    sector = _check_sector(sector, product=product, sensor=sensor)
    start_time, end_time = _check_start_end_time(start_time, end_time)

    filter_parameters = _check_filter_parameters(
        filter_parameters, sensor, sector=sector
    )
    group_by_key = _check_group_by_key(group_by_key)

    # Add start_time and end_time to filter_parameters
    filter_parameters = filter_parameters.copy()
    filter_parameters["start_time"] = start_time
    filter_parameters["end_time"] = end_time

    # Get filesystem
    fs = get_filesystem(protocol=protocol, fs_args=fs_args)

    bucket_prefix = _get_bucket_prefix(protocol)

    # Get product dir
    product_dir = _get_product_dir(
        protocol=protocol,
        base_dir=base_dir,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
    )

    # Define time directories
    start_year, start_doy, start_hour = _dt_to_year_doy_hour(start_time)
    end_year, end_doy, end_hour = _dt_to_year_doy_hour(end_time)
    list_hourly_times = pd.date_range(start_time, end_time, freq="1h")
    list_year_doy_hour = [_dt_to_year_doy_hour(dt) for dt in list_hourly_times]
    list_year_doy_hour = ["/".join(tpl) for tpl in list_year_doy_hour]

    # Define glob patterns
    list_glob_pattern = [
        os.path.join(product_dir, dt_str, "*.nc*") for dt_str in list_year_doy_hour
    ]
    n_directories = len(list_glob_pattern)
    if verbose:
        print(f"Searching files across {n_directories} directories.")

    # Loop over each directory:
    # - TODO in parallel ?
    list_fpaths = []
    # glob_pattern = list_glob_pattern[0]
    for glob_pattern in list_glob_pattern:
        # Retrieve list of files
        fpaths = fs.glob(glob_pattern)
        # Add bucket prefix
        fpaths = [bucket_prefix + fpath for fpath in fpaths]
        # Filter files if necessary
        if len(filter_parameters) >= 1:
            fpaths = _filter_files(fpaths, sensor, product_level, **filter_parameters)
        list_fpaths += fpaths

    fpaths = list_fpaths

    # Group fpaths by key
    if group_by_key:
        fpaths = _group_fpaths_by_key(fpaths, sensor, product_level, key=group_by_key)
    # Parse fpaths for connection type
    fpaths = _set_connection_type(
        fpaths, satellite=satellite, protocol=protocol, connection_type=connection_type
    )
    # Return fpaths
    return fpaths


def find_closest_start_time(
    time,
    satellite,
    sensor,
    product_level,
    product,
    sector=None, 
    base_dir=None,
    protocol=None,
    fs_args={},
    filter_parameters={},
):
    """
    Retrieve files start_time closest to the specified time.

    Parameters
    ----------
    time : datetime.datetime
        The time for which you desire to know the closest file start_time.
    base_dir : str
        Base directory path where the <HIMAWARI-**> satellite is located.
        This argument must be specified only if searching files on local storage.
        If it is specified, protocol and fs_args arguments must not be specified.
    protocol : str
        String specifying the cloud bucket storage from which to retrieve
        the data. It must be specified if not searching data on local storage.
        Use `himawari_api.available_protocols()` to retrieve available protocols.
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    satellite : str
        The name of the satellite.
        Use `himawari_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `himawari_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `himawari_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `himawari_api.available_products()` for a list of available products.
    sector : str
        The acronym of the AHI sector for which to retrieve the files.
        See `himawari_api.available_sectors()` for a list of available sectors.
    filter_parameters: dict, optional
        Dictionary specifying option filtering parameters.
        Valid keys includes: `channels`, `scan_modes`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    """
    # Set time precision to minutes
    time = _check_time(time)
    time = time.replace(microsecond=0, second=0)
    # Retrieve timedelta conditioned to sector (for AHI)
    timedelta = _get_acquisition_max_timedelta(sector)
    # Define start_time and end_time
    start_time = time - timedelta
    end_time = time + timedelta
    # Retrieve files
    fpath_dict = find_files(
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        start_time=start_time,
        end_time=end_time,
        filter_parameters=filter_parameters,
        group_by_key="start_time",
        verbose=False,
    )
    # Select start_time closest to time
    list_datetime = sorted(list(fpath_dict.keys()))
    if len(list_datetime) == 0:
        dt_str = int(timedelta.seconds / 60)
        raise ValueError(
            f"No data available in previous and next {dt_str} minutes around {time}."
        )
    idx_closest = np.argmin(np.abs(np.array(list_datetime) - time))
    datetime_closest = list_datetime[idx_closest]
    return datetime_closest


def find_latest_start_time(
    satellite,
    sensor,
    product_level,
    product,
    sector=None, 
    connection_type=None,
    base_dir=None,
    protocol=None,
    fs_args={},
    filter_parameters={},
    look_ahead_minutes=30,
):
    """
    Retrieve the latest file start_time available.

    Parameters
    ----------
    look_ahead_minutes: int, optional
        Number of minutes before actual time to search for latest data.
        THe default is 30 minutes.
    base_dir : str
        Base directory path where the <HIMAWARI-**> satellite is located.
        This argument must be specified only if searching files on local storage.
        If it is specified, protocol and fs_args arguments must not be specified.
    protocol : str
        String specifying the cloud bucket storage from which to retrieve
        the data. It must be specified if not searching data on local storage.
        Use `himawari_api.available_protocols()` to retrieve available protocols.
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    satellite : str
        The name of the satellite.
        Use `himawari_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `himawari_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `himawari_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `himawari_api.available_products()` for a list of available products.
    sector : str
        The acronym of the AHI sector for which to retrieve the files.
        See `himawari_api.available_sectors()` for a list of available sectors.
    filter_parameters: dict, optional
        Dictionary specifying option filtering parameters.
        Valid keys includes: `channels`, `scan_modes`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    """
    # Search in the past N hour of data
    start_time = datetime.datetime.utcnow() - datetime.timedelta(
        minutes=look_ahead_minutes
    )
    end_time = datetime.datetime.utcnow()
    fpath_dict = find_files(
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        start_time=start_time,
        end_time=end_time,
        filter_parameters=filter_parameters,
        group_by_key="start_time",
        connection_type=connection_type,
        verbose=False,
    )
    # Find the latest time available
    if len(fpath_dict) == 0: 
        raise ValueError("No data found. Maybe try to increase `look_ahead_minutes`.")
    list_datetime = list(fpath_dict.keys())
    idx_latest = np.argmax(np.array(list_datetime))
    datetime_latest = list_datetime[idx_latest]
    return datetime_latest


def find_closest_files(
    time,
    satellite,
    sensor,
    product_level,
    product,
    sector=None, 
    connection_type=None,
    base_dir=None,
    protocol=None,
    fs_args={},
    filter_parameters={},
):
    """
    Retrieve files closest to the specified time.

    If you are querying mesoscale domain data (sector=M), you might be
      interested to specify in the filter_parameters dictionary the
      key `scene_abbr` with values "M1" or "M2".

    Parameters
    ----------
    base_dir : str
        Base directory path where the <HIMAWARI-**> satellite is located.
        This argument must be specified only if searching files on local storage.
        If it is specified, protocol and fs_args arguments must not be specified.
    protocol : str
        String specifying the cloud bucket storage from which to retrieve
        the data. It must be specified if not searching data on local storage.
        Use `himawari_api.available_protocols()` to retrieve available protocols.
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    satellite : str
        The name of the satellite.
        Use `himawari_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `himawari_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `himawari_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `himawari_api.available_products()` for a list of available products.
    sector : str
        The acronym of the AHI sector for which to retrieve the files.
        See `himawari_api.available_sectors()` for a list of available sectors.
    time : datetime.datetime
        The time for which you desire to retrieve the files with closest start_time.
    filter_parameters : dict, optional
        Dictionary specifying option filtering parameters.
        Valid keys includes: `channels`, `scan_modes`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    connection_type : str, optional
        The type of connection to a cloud bucket.
        This argument applies only if working with cloud buckets (base_dir is None).
        See `himawari_api.available_connection_types` for implemented solutions.

    """
    # Set time precision to minutes
    time = _check_time(time)
    time = time.replace(microsecond=0, second=0)
    # Retrieve timedelta conditioned to sector type
    timedelta = _get_acquisition_max_timedelta(sector)
    # Define start_time and end_time
    start_time = time - timedelta
    end_time = time + timedelta
    # Retrieve files
    fpath_dict = find_files(
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        start_time=start_time,
        end_time=end_time,
        filter_parameters=filter_parameters,
        group_by_key="start_time",
        verbose=False,
    )
    # Select start_time closest to time
    list_datetime = sorted(list(fpath_dict.keys()))
    if len(list_datetime) == 0:
        dt_str = int(timedelta.seconds / 60)
        raise ValueError(
            f"No data available in previous and next {dt_str} minutes around {time}."
        )
    idx_closest = np.argmin(np.abs(np.array(list_datetime) - time))
    datetime_closest = list_datetime[idx_closest]
    return fpath_dict[datetime_closest]


def find_latest_files(
    satellite,
    sensor,
    product_level,
    product,
    sector=None, 
    connection_type=None,
    base_dir=None,
    protocol=None,
    fs_args={},
    filter_parameters={},
    N = 1, 
    check_consistency=True, 
    look_ahead_minutes=30,
):
    """
    Retrieve latest available files.

    If you are querying mesoscale domain data (sector=M), you might be
      interested to specify in the filter_parameters dictionary the
      key `scene_abbr` with values "M1" or "M2".

    Parameters
    ----------
    look_ahead_minutes: int, optional
        Number of minutes before actual time to search for latest data.
        The default is 30 minutes.
    N : int
        The number of last timesteps for which to download the files.
        The default is 1.
    check_consistency : bool, optional
        Check for consistency of the returned files. The default is True.
        It check that:
         - the regularity of the previous timesteps, with no missing timesteps;
         - the regularity of the scan mode, i.e. not switching from M3 to M6,
         - if sector == M, the mesoscale domains are not changing within the considered period.
    base_dir : str
        Base directory path where the <HIMAWARI-**> satellite is located.
        This argument must be specified only if searching files on local storage.
        If it is specified, protocol and fs_args arguments must not be specified.
    protocol : str
        String specifying the cloud bucket storage from which to retrieve
        the data. It must be specified if not searching data on local storage.
        Use `himawari_api.available_protocols()` to retrieve available protocols.
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    satellite : str
        The name of the satellite.
        Use `himawari_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `himawari_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `himawari_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `himawari_api.available_products()` for a list of available products.
    sector : str
        The acronym of the AHI sector for which to retrieve the files.
        See `himawari_api.available_sectors()` for a list of available sectors.
    filter_parameters : dict, optional
        Dictionary specifying option filtering parameters.
        Valid keys includes: `channels`, `scan_modes`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    connection_type : str, optional
        The type of connection to a cloud bucket.
        This argument applies only if working with cloud buckets (base_dir is None).
        See `himawari_api.available_connection_types` for implemented solutions.

    """
    # Get closest time
    latest_time = find_latest_start_time(
        look_ahead_minutes=look_ahead_minutes, 
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
    )
    
    fpath_dict = find_previous_files(
        N = N, 
        check_consistency=check_consistency,
        start_time=latest_time,
        include_start_time=True, 
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
        connection_type=connection_type,
    )
    return fpath_dict


def find_previous_files(
    start_time,
    N,
    satellite,
    sensor,
    product_level,
    product,
    sector=None, 
    filter_parameters={},
    connection_type=None,
    base_dir=None,
    protocol=None,
    fs_args={},
    include_start_time=False,
    check_consistency=True,
):
    """
    Find files for N timesteps previous to start_time.

    Parameters
    ----------
    start_time : datetime
        The start_time from which to search for previous files.
        The start_time should correspond exactly to file start_time if check_consistency=True
    N : int
        The number of previous timesteps for which to retrieve the files.
    include_start_time: bool, optional
        Wheter to include (and count) start_time in the N returned timesteps.
        The default is False.
    check_consistency : bool, optional
        Check for consistency of the returned files. The default is True.
        It check that:
         - start_time correspond exactly to the start_time of the files;
         - the regularity of the previous timesteps, with no missing timesteps;
         - the regularity of the scan mode, i.e. not switching from M3 to M6,
         - if sector == M, the mesoscale domains are not changing within the considered period.
    base_dir : str
        Base directory path where the <HIMAWARI-**> satellite is located.
        This argument must be specified only if searching files on local storage.
        If it is specified, protocol and fs_args arguments must not be specified.
    protocol : str
        String specifying the cloud bucket storage from which to retrieve
        the data. It must be specified if not searching data on local storage.
        Use `himawari_api.available_protocols()` to retrieve available protocols.
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    satellite : str
        The name of the satellite.
        Use `himawari_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `himawari_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `himawari_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `himawari_api.available_products()` for a list of available products.
    sector : str
        The acronym of the AHI sector for which to retrieve the files.
        See `himawari_api.available_sectors()` for a list of available sectors.
    filter_parameters : dict, optional
        Dictionary specifying option filtering parameters.
        Valid keys includes: `channels`, `scan_modes`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    connection_type : str, optional
        The type of connection to a cloud bucket.
        This argument applies only if working with cloud buckets (base_dir is None).
        See `himawari_api.available_connection_types` for implemented solutions.

    Returns
    -------
    fpath_dict : dict
        Dictionary with structure {<datetime>: [fpaths]}

    """
    sensor = _check_sensor(sensor)
    sector = _check_sector(sector, sensor=sensor)
    product_level = _check_product_level(product_level)
    # Set time precision to minutes
    start_time = _check_time(start_time)
    start_time = start_time.replace(microsecond=0, second=0)
    # Get closest time and check is as start_time (otherwise warning)
    closest_time = find_closest_start_time(
        time=start_time,
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
    )
    # Check start_time is the precise start_time of the file
    if check_consistency and closest_time != start_time:
        raise ValueError(
            f"start_time='{start_time}' is not an actual start_time. "
            f"The closest start_time is '{closest_time}'"
        )
    # Retrieve timedelta conditioned to sector type
    timedelta = _get_acquisition_max_timedelta(sector)
    # Define start_time and end_time
    start_time = closest_time - timedelta * (N+1) # +1 for when include_start_time=False
    end_time = closest_time
    # Retrieve files
    fpath_dict = find_files(
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        start_time=start_time,
        end_time=end_time,
        filter_parameters=filter_parameters,
        group_by_key="start_time",
        connection_type=connection_type,
        verbose=False,
    )
    # List previous datetime
    list_datetime = sorted(list(fpath_dict.keys()))
    # Remove start_time if include_start_time=False
    if not include_start_time:
        list_datetime.remove(closest_time)
    list_datetime = sorted(list_datetime)
    # Check data availability
    if len(list_datetime) == 0:
        raise ValueError(f"No data available between {start_time} and {end_time}.")
    if len(list_datetime) < N:
        raise ValueError(
            f"No {N} timesteps available between {start_time} and {end_time}."
        )
    # Select N most recent start_time
    list_datetime = list_datetime[-N:]
    # Select files for N most recent start_time
    fpath_dict = {tt: fpath_dict[tt] for tt in list_datetime}
    # ----------------------------------------------------------
    # Perform consistency checks
    if check_consistency:
        # Check constant scan_mode
        _check_unique_scan_mode(fpath_dict, sensor, product_level)
        # Check for interval regularity
        if not include_start_time: 
            list_datetime = list_datetime + [closest_time]
        _check_interval_regularity(list_datetime)
        # TODO Check for Mesoscale same location (on M1 and M2 separately) !
        # - raise information when it changes !
        if sector == "M":
            pass
    # ----------------------------------------------------------
    # Return files dictionary
    return fpath_dict


def find_next_files(
    start_time,
    N,
    satellite,
    sensor,
    product_level,
    product,
    sector=None, 
    filter_parameters={},
    connection_type=None,
    base_dir=None,
    protocol=None,
    fs_args={},
    include_start_time=False,
    check_consistency=True,
):
    """
    Find files for N timesteps after start_time.

    Parameters
    ----------
    start_time : datetime
        The start_time from which search for next files.
        The start_time should correspond exactly to file start_time if check_consistency=True
    N : int
        The number of next timesteps for which to retrieve the files.
    include_start_time: bool, optional
        Wheter to include (and count) start_time in the N returned timesteps.
        The default is False.
    check_consistency : bool, optional
        Check for consistency of the returned files. The default is True.
        It check that:
         - start_time correspond exactly to the start_time of the files;
         - the regularity of the previous timesteps, with no missing timesteps;
         - the regularity of the scan mode, i.e. not switching from M3 to M6,
         - if sector == M, the mesoscale domains are not changing within the considered period.
    base_dir : str
        Base directory path where the <HIMAWARI-**> satellite is located.
        This argument must be specified only if searching files on local storage.
        If it is specified, protocol and fs_args arguments must not be specified.
    protocol : str
        String specifying the cloud bucket storage from which to retrieve
        the data. It must be specified if not searching data on local storage.
        Use `himawari_api.available_protocols()` to retrieve available protocols.
    fs_args : dict, optional
        Dictionary specifying optional settings to initiate the fsspec.filesystem.
        The default is an empty dictionary. Anonymous connection is set by default.
    satellite : str
        The name of the satellite.
        Use `himawari_api.available_satellites()` to retrieve the available satellites.
    sensor : str
        Satellite sensor.
        See `himawari_api.available_sensors()` for available sensors.
    product_level : str
        Product level.
        See `himawari_api.available_product_levels()` for available product levels.
    product : str
        The name of the product to retrieve.
        See `himawari_api.available_products()` for a list of available products.
    sector : str
        The acronym of the AHI sector for which to retrieve the files.
        See `himawari_api.available_sectors()` for a list of available sectors.
    filter_parameters : dict, optional
        Dictionary specifying option filtering parameters.
        Valid keys includes: `channels`, `scan_modes`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    connection_type : str, optional
        The type of connection to a cloud bucket.
        This argument applies only if working with cloud buckets (base_dir is None).
        See `himawari_api.available_connection_types` for implemented solutions.

    Returns
    -------
    fpath_dict : dict
        Dictionary with structure {<datetime>: [fpaths]}

    """
    sensor = _check_sensor(sensor)
    sector = _check_sector(sector, sensor=sensor)
    product_level = _check_product_level(product_level)
    # Set time precision to minutes
    start_time = _check_time(start_time)
    start_time = start_time.replace(microsecond=0, second=0)
    # Get closest time and check is as start_time (otherwise warning)
    closest_time = find_closest_start_time(
        time=start_time,
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        filter_parameters=filter_parameters,
    )
    # Check start_time is the precise start_time of the file
    if check_consistency and closest_time != start_time:
        raise ValueError(
            f"start_time='{start_time}' is not an actual start_time. "
            f"The closest start_time is '{closest_time}'"
        )
    # Retrieve timedelta conditioned to sector type
    timedelta = _get_acquisition_max_timedelta(sector)
    # Define start_time and end_time
    start_time = closest_time
    end_time = closest_time + timedelta * (N+1) # +1 for when include_start_time=False
    # Retrieve files
    fpath_dict = find_files(
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
        sensor=sensor,
        product_level=product_level,
        product=product,
        sector=sector,
        start_time=start_time,
        end_time=end_time,
        filter_parameters=filter_parameters,
        group_by_key="start_time",
        connection_type=connection_type,
        verbose=False,
    )
    # List previous datetime
    list_datetime = sorted(list(fpath_dict.keys()))
    if not include_start_time:
        list_datetime.remove(closest_time)
    list_datetime = sorted(list_datetime)
    # Check data availability
    if len(list_datetime) == 0:
        raise ValueError(f"No data available between {start_time} and {end_time}.")
    if len(list_datetime) < N:
        raise ValueError(
            f"No {N} timesteps available between {start_time} and {end_time}."
        )
    # Select N most recent start_time
    list_datetime = list_datetime[0:N]
    # Select files for N most recent start_time
    fpath_dict = {tt: fpath_dict[tt] for tt in list_datetime}
    # ----------------------------------------------------------
    # Perform consistency checks
    if check_consistency:
        # Check constant scan_mode
        _check_unique_scan_mode(fpath_dict, sensor, product_level)
        # Check for interval regularity
        if not include_start_time: 
            list_datetime = list_datetime + [closest_time]
        _check_interval_regularity(list_datetime)
        # TODO Check for Mesoscale same location (on M1 and M2 separately) !
        # - raise information when it changes !
        if sector == "M":
            pass
    # ----------------------------------------------------------
    # Return files dictionary
    return fpath_dict


####--------------------------------------------------------------------------.
#### Output options
def _add_nc_bytes(fpaths):
    """Add `#mode=bytes` to the HTTP netCDF4 url."""
    fpaths = [fpath + "#mode=bytes" for fpath in fpaths]
    return fpaths


def _set_connection_type(fpaths, satellite, protocol=None, connection_type=None):
    """Switch from bucket to https connection for protocol 's3'."""
    if protocol is None:
        return fpaths
    if protocol == "file":
        return fpaths
    # here protocol s3
    if connection_type == "bucket":
        return fpaths
    if connection_type in ["https", "nc_bytes"]:
        if isinstance(fpaths, list):
            fpaths = _switch_to_https_fpaths(fpaths, protocol=protocol)
            if connection_type == "nc_bytes":
                fpaths = _add_nc_bytes(fpaths)
        if isinstance(fpaths, dict):
            fpaths = {
                tt: _switch_to_https_fpaths(l_fpaths, protocol=protocol)           
                for tt, l_fpaths in fpaths.items()
            }
            if connection_type == "nc_bytes":
                fpaths = {
                    tt: _add_nc_bytes(l_fpaths) for tt, l_fpaths in fpaths.items()
                }
        return fpaths
    else:
        raise NotImplementedError(
            "'bucket','https', 'nc_bytes' are the only `connection_type` available."
        )
