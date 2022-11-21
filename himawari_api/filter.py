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

from himawari_api.checks import (
     _check_channels,
     _check_scene_abbr,
     _check_start_end_time,
     _check_product_level,
)
from himawari_api.info import _get_info_from_filepath


def _filter_file(
    fpath,
    product,
    product_level,
    start_time=None,
    end_time=None,
    channels=None,
    scene_abbr=None,
):
    """Utility function to filter a filepath based on optional filter_parameters."""
    # scene_abbr and channels must be list, start_time and end_time a datetime object
    # TODO: Currently no way to filter R1 and R2 (I think inside a single bz2 file.)
    # TODO: Currently R4 and R5 are not on AWS 

    # Get info from filepath
    info_dict = _get_info_from_filepath(fpath)

    # Filter by channels
    if channels is not None:
        file_channel = info_dict.get("channel")
        if file_channel is not None:
            if file_channel not in channels:
                return None

    # Filter by scene_abbr
    if scene_abbr is not None:
        file_scene_abbr = info_dict.get("scene_abbr")
        if file_scene_abbr is not None:
            if file_scene_abbr not in scene_abbr:
                return None
    
    # Filter by start_time
    if start_time is not None:
        # If the file ends before (or at) start_time, do not select
        file_end_time = info_dict.get("end_time")
        if file_end_time <= start_time: 
            return None
        
        # This could exclude a file with 'start_time' within the file
        # if file_start_time < start_time:
        #     return None

    # Filter by end_time
    if end_time is not None:
        file_start_time = info_dict.get("start_time")
        # If the file starts after end_time, do not select
        # If >= it excludes files with start_time == end_time ! 
        if file_start_time > end_time:
            return None
        # This could exclude a file with end_time within the file
        # if file_end_time > end_time:
        #     return None
        
    # Filter by product 
    if product != info_dict.get("product"):
        return None 
    
    return fpath


def _filter_files(
    fpaths,
    product, 
    product_level,
    start_time=None,
    end_time=None,
    channels=None,
    scene_abbr=None,
):
    """Utility function to select filepaths matching optional filter_parameters."""
    if isinstance(fpaths, str):
        fpaths = [fpaths]
    fpaths = [
        _filter_file(
            fpath,
            product, 
            product_level,
            start_time=start_time,
            end_time=end_time,
            channels=channels,
            scene_abbr=scene_abbr,
        )
        for fpath in fpaths
    ]
    fpaths = [fpath for fpath in fpaths if fpath is not None]
    return fpaths


def filter_files(
    fpaths,
    product, 
    product_level,
    start_time=None,
    end_time=None,
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
    product_level : str
        Product level.
        See `himawari_api.available_product_levels()` for available product levels.
    start_time : datetime.datetime, optional
        Time defining interval start.
        The default is None (no filtering by start_time).
    end_time : datetime.datetime, optional
        Time defining interval end.
        The default is None (no filtering by end_time).
    scene_abbr : str, optional
        String specifying selection of Japan, Target, or Landmark scan region.
        Either R1 or R2 for sector Japan, R3 for Target, R4 or R5 for Landmark.
        The default is None (no filtering by scan region).
    channels : list, optional
        List of AHI channels to select.
        See `himawari_api.available_channels()` for available AHI channels.
        The default is None (no filtering by channels).

    """
    product_level = _check_product_level(product_level, product=None)
    channels = _check_channels(channels)
    scene_abbr = _check_scene_abbr(scene_abbr)
    start_time, end_time = _check_start_end_time(start_time, end_time)
    fpaths = _filter_files(
        fpaths=fpaths,
        product=product, 
        product_level=product_level,
        start_time=start_time,
        end_time=end_time,
        channels=channels,
        scene_abbr=scene_abbr,
    )
    return fpaths

