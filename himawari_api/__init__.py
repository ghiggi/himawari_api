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

from .info import (
    available_protocols,
    available_satellites,
    available_sectors,
    available_product_levels,
    available_channels,
    available_products,
    available_connection_types,
    available_group_keys,
    group_files,
)
from .search import (
    find_closest_start_time,
    find_latest_start_time,
    find_files,
    find_closest_files,
    find_latest_files,
    find_previous_files,
    find_next_files,
)
from .download import (
    download_files,
    download_closest_files,
    download_latest_files,
    download_next_files,
    download_previous_files,
)
from .filter import filter_files
from .explore import (
    open_directory_explorer,
    open_ahi_channel_guide,
)

__all__ = [
    "available_protocols",
    "available_satellites",
    "available_sectors",
    "available_product_levels",
    "available_products",
    "available_channels",
    "available_connection_types",
    "available_group_keys",
    "download_files",
    "download_closest_files",
    "download_latest_files",
    "download_next_files",
    "download_previous_files",
    "find_files",
    "find_latest_files",
    "find_closest_files",
    "find_previous_files",
    "find_next_files",
    "group_files",
    "filter_files",
    "find_closest_start_time",
    "find_latest_start_time",
    "open_directory_explorer",
    "open_ahi_channel_guide",
]
