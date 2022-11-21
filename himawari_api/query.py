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
"""Wrappers to extract information from filepaths."""

from .info import get_key_from_filepaths


def product_level(fpaths):
    return get_key_from_filepaths(fpaths, key="product_level")


def product(fpaths):
    return get_key_from_filepaths(fpaths, key="product")


def sector(fpaths):
    return get_key_from_filepaths(fpaths, key="sector")


def scene_abbr(fpaths):
    return get_key_from_filepaths(fpaths, key="scene_abbr")


def channel(fpaths):
    return get_key_from_filepaths(fpaths, key="channel")


def start_time(fpaths):
    return get_key_from_filepaths(fpaths, key="start_time")


def end_time(fpaths):
    return get_key_from_filepaths(fpaths, key="end_time")


def satellite(fpaths):
    return get_key_from_filepaths(fpaths, key="satellite")



