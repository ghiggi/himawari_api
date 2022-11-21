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
"""Function for searching files."""

import os
import datetime
import numpy as np
import pandas as pd
from himawari_api.info import _group_fpaths_by_key
from himawari_api.filter import _filter_files
from himawari_api.checks import (
     _check_protocol,
     _check_base_dir,
     _check_connection_type,
     _check_satellite,
     _check_product_level,
     _check_product,
     _check_sector,
     _check_time,
     _check_start_end_time,
     _check_filter_parameters,
     _check_group_by_key,
     _check_interval_regularity,
)
from himawari_api.io import (
    _set_connection_type,
    _get_product_dir,
    _get_bucket_prefix,
    get_filesystem,
    get_fname_glob_pattern,
)


def _get_acquisition_max_timedelta(sector):
    """Get reasonable timedelta based on AHI sector to find previous/next acquisition."""
    if sector == "Target":
       dt = datetime.timedelta(minutes=2, seconds=30)     
    elif sector == "Japan":
        dt = datetime.timedelta(minutes=2, seconds=30)    
    elif sector == "FLDK":
        dt = datetime.timedelta(minutes=10)   
    elif sector == "Landmark": 
        dt = datetime.timedelta(seconds=30)    
    else:
        raise ValueError("Unknown sector.")
    return dt


def _dt_to_year_month_day_hhmm(dt):
    """Return (year, month, day, hhmm) strings from datetime object."""
    year = dt.strftime("%Y")                # year
    month = dt.strftime("%m").ljust(2,"0")  # month 
    day = dt.strftime("%d").ljust(2,"0")    # day 
    hour = dt.strftime("%H").ljust(2,"0")   # hh
    minute = int(dt.strftime("%M"))
    # Round minute down to nearest 10 multiplier 
    # - 00, 10, 20, 30, 40, 50 
    minute = int(np.floor(minute/ 10) * 10)
    minute_str = str(minute).ljust(2,"0") 
    # Define hhmm string 
    hhmm = hour + minute_str    
    return year, month, day, hhmm


def find_files(
    satellite,
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

    If you are querying data from sector 'Japan', 'Target' or 'Landmark', you might be
      interested to specify in the filter_parameters dictionary the
      key `scene_abbr` with values "R1", "R2" (for Japan), "R3" (for Target") or  
     "R4" and "R5" (for Landmark).
      
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
        Valid keys includes: `channels`, `scene_abbr`.
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
    product_level = _check_product_level(product_level, product=None)
    product = _check_product(product, product_level=product_level)
    sector = _check_sector(sector, product=product)  
    start_time, end_time = _check_start_end_time(start_time, end_time) 
    filter_parameters = _check_filter_parameters(filter_parameters, sector=sector)
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
        product_level=product_level,
        product=product,
        sector=sector,
    )

    # Define time directories 
    # <YYYY>/<MM>/<DD>/<HH00, HH10, HH20,...>)
    # - start_time =  datetime.datetime(2022, 11, 21, 15, 27, 30)
    # - end_time =  datetime.datetime(2022, 11, 21, 15, 27, 30)
    # --> + timedelta is required to ensure correct list 
    list_timesteps = pd.date_range(start_time, end_time + datetime.timedelta(minutes=10), freq="10min")
    list_time_dir_tree = ["/".join(_dt_to_year_month_day_hhmm(dt)) for dt in list_timesteps]

    # Define glob patterns 
    fname_glob_pattern = get_fname_glob_pattern(product_level=product_level)
    list_glob_pattern = [os.path.join(product_dir, time_dir_tree, fname_glob_pattern) for time_dir_tree in list_time_dir_tree]
    n_directories = len(list_glob_pattern)
    if verbose:
        print(f"Searching files across {n_directories} directories.")

    # Loop over each directory:
    # - TODO in parallel 
    list_fpaths = []
    # glob_pattern = list_glob_pattern[0]
    for glob_pattern in list_glob_pattern:
        # Retrieve list of files
        fpaths = fs.glob(glob_pattern)
        # Add bucket prefix
        fpaths = [bucket_prefix + fpath for fpath in fpaths]
        # Filter files if necessary
        if len(filter_parameters) >= 1:
            fpaths = _filter_files(fpaths, product, product_level, **filter_parameters)  
        list_fpaths += fpaths

    fpaths = list_fpaths

    # Group fpaths by key
    if group_by_key:
        fpaths = _group_fpaths_by_key(fpaths, product_level, key=group_by_key)  
        
    # Parse fpaths for connection type
    fpaths = _set_connection_type(
        fpaths, satellite=satellite, protocol=protocol, connection_type=connection_type
    )
    # Return fpaths
    return fpaths


def find_closest_start_time(
    time,
    satellite,
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
        Valid keys includes: `channels`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    """
    # Set time precision to minutes
    time = _check_time(time)
    # Retrieve timedelta conditioned to sector (for AHI)
    sector = _check_sector(sector)
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
        Valid keys includes: `channels`, `scene_abbr`.
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
        Valid keys includes: `channels`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    connection_type : str, optional
        The type of connection to a cloud bucket.
        This argument applies only if working with cloud buckets (base_dir is None).
        See `himawari_api.available_connection_types` for implemented solutions.

    """
    # Set time precision to minutes
    time = _check_time(time)
    # Retrieve timedelta conditioned to sector type
    sector = _check_sector(sector)
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
        Valid keys includes: `channels`, `scene_abbr`.
        The default is a empty dictionary (no filtering).
    connection_type : str, optional
        The type of connection to a cloud bucket.
        This argument applies only if working with cloud buckets (base_dir is None).
        See `himawari_api.available_connection_types` for implemented solutions.

    """
    import time
    # Get closest time
    for i in range(10):
        try:
            latest_time = find_latest_start_time(
                look_ahead_minutes=look_ahead_minutes, 
                base_dir=base_dir,
                protocol=protocol,
                fs_args=fs_args,
                satellite=satellite,
                product_level=product_level,
                product=product,
                sector=sector,
                filter_parameters=filter_parameters,
            )
            break
        except: 
            time.sleep(1)
            if i == 9:
                raise ValueError("Impossible to retrieve last timestep available.")

    fpath_dict = find_previous_files(
        N = N, 
        check_consistency=check_consistency,
        start_time=latest_time,
        include_start_time=True, 
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
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
        Valid keys includes: `channels`, `scene_abbr`.
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
    sector = _check_sector(sector)
    product_level = _check_product_level(product_level)
    # Set time precision to seconds
    start_time = _check_time(start_time)
    # Get closest time and check is as start_time (otherwise warning)
    closest_time = find_closest_start_time(
        time=start_time,
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
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
        # Check for interval regularity
        if not include_start_time: 
            list_datetime = list_datetime + [closest_time]
        _check_interval_regularity(list_datetime)

    # ----------------------------------------------------------
    # Return files dictionary
    return fpath_dict


def find_next_files(
    start_time,
    N,
    satellite,
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
        Valid keys includes: `channels`, `scene_abbr`.
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
    sector = _check_sector(sector)
    product_level = _check_product_level(product_level)
    # Set time precision to seconds
    start_time = _check_time(start_time)
    # Get closest time and check is as start_time (otherwise warning)
    closest_time = find_closest_start_time(
        time=start_time,
        base_dir=base_dir,
        protocol=protocol,
        fs_args=fs_args,
        satellite=satellite,
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
        # Check for interval regularity
        if not include_start_time: 
            list_datetime = list_datetime + [closest_time]
        _check_interval_regularity(list_datetime)

    # ----------------------------------------------------------
    # Return files dictionary
    return fpath_dict


