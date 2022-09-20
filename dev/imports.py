#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  6 19:04:15 2022

@author: ghiggi
"""
import os
import datetime
import fsspec
import dask
import pandas as pd
import pandas as np

from goes_api.utils.time import (
    _dt_to_year_doy_hour,
    get_end_of_day,
    get_list_daily_time_blocks,
    get_start_of_day,
)
from goes_api.io import (
    _add_nc_bytes,
    _channels,
    _check_base_dir,
    _check_channel,
    _check_channels,
    _check_connection_type,
    _check_filter_parameters,
    _check_group_by_key,
    _check_interval_regularity,
    _check_product,
    _check_product_level,
    _check_product_levels,
    _check_protocol,
    _check_satellite,
    _check_scan_mode,
    _check_scan_modes,
    _check_scene_abbr,
    _check_sector,
    _check_sensor,
    _check_sensors,
    _check_start_end_time,
    _check_time,
    _check_unique_scan_mode,
    _dt_to_year_doy_hour,
    _filter_file,
    _filter_files,
    _get_bucket_prefix,
    _get_info_from_filename,
    _get_info_from_filepath,
    _get_key_from_filepaths,
    _get_product_dir,
    get_dict_product_level_products,
    _get_product_name,
    # _get_products,
    get_dict_info_products,
    get_dict_product_product_level,
    get_dict_product_sensor,
    _get_sector_timedelta,
    get_dict_sensor_products,
    _group_fpaths_by_key,
    _satellites,
    _sectors,
    _separate_product_scene_abbr,
    _set_connection_type,
    _switch_to_https_fpaths,
    available_channels,
    available_product_levels,
    available_products,
    available_satellites,
    available_scan_modes,
    available_sectors,
    available_sensors,
    filter_files,
    find_closest_files,
    find_closest_start_time,
    find_files,
    find_latest_files,
    find_latest_start_time,
    find_next_files,
    find_previous_files,
    # get_ABI_channel_info,
    get_available_online_product,
    get_bucket,
    get_filesystem,
    group_files,
)
from goes_api.download import (
    _check_download_protocol,
    _fs_get_parallel,
    _get_local_from_bucket_fpaths,
    remove_bucket_address,
    _select_missing_fpaths,
    create_local_directories,
    download_closest_files,
    download_files,
    download_latest_files,
    download_next_files,
    download_previous_files,
)
