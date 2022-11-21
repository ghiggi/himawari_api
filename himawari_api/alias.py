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
"""Define input argument aliases."""

_satellites = {
    "himawari-8": ["H8", "H08", "HIMAWARI-8", "HIMAWARI8"],
    "himawari-9": ["H9", "H08", "HIMAWARI-9", "HIMAWARI9"],
}

_sectors = {
    "FLDK": ["FLDK", "FULL", "FULLDISK", "FULL DISK", "F"],
    "Japan": ["JAPAN", "JAPAN_AREA", "JAPAN AREA", "J"],                         
    "Target": ["TARGET", "TARGET_AREA", "TARGET AREA", "T"],
    "Landmark": ["LANDMARK", "M", "MESOSCALE"],
}


# - Channel informations : https://www.data.jma.go.jp/mscweb/en/himawari89/space_segment/spsg_ahi.html
_channels = {
    "B01": ["B01", "C01", "1", "01", "0.47", "0.46", "BLUE", "B"],
    "B02": ["B02", "C02", "2", "02", "0.51", "RED", "R"],
    "B03": ["B03", "C03", "3", "03", "0.64", "GREEN", "G"],                                 
    "B04": ["B04", "C04", "4", "04", "0.86", "CIRRUS"],
    "B05": ["B05", "C05", "5", "05", "1.6", "SNOW/ICE"],
    "B06": ["B06", "C06", "6", "06", "2.3", "CLOUD PARTICLE SIZE", "CPS"],
    "B07": ["B07", "C07", "7", "07", "3.9", "IR SHORTWAVE WINDOW", "IR SHORTWAVE"],
    "B08": ["B08", "C08", "8", "08", "6.2", "UPPER-LEVEL TROPOSPHERIC WATER VAPOUR",  "UPPER-LEVEL WATER VAPOUR"],
    "B09": ["B09", "C09", "9", "09", "6.9", "7.0", "MID-LEVEL TROPOSPHERIC WATER VAPOUR", "MID-LEVEL WATER VAPOUR"],
    "B10": ["B10", "C10", "10", "10", "7.3", "LOWER-LEVEL TROPOSPHERIC WATER VAPOUR", "LOWER-LEVEL WATER VAPOUR"],
    "B11": ["B11", "C11", "11", "11", "8.6", "CLOUD-TOP PHASE", "CTP"],
    "B12": ["B12", "C12", "12", "12", "9.6", "OZONE"],
    "B13": ["B13", "C13", "13", "10.4", "CLEAN IR LONGWAVE WINDOW", "CLEAN IR"],
    "B14": ["B14", "C14", "14", "11.2", "IR LONGWAVE WINDOW", "IR LONGWAVE"],
    "B15": ["B16","C15", "15", "12.3", "12.4", "DIRTY LONGWAVE WINDOW", "DIRTY IR"],
    "B16": ["B16","C16", "16", "13.3", "CO2 IR LONGWAVE", "CO2", "CO2 IR"],
}

PROTOCOLS = ["s3", "local", "file"]
BUCKET_PROTOCOLS = ["s3"]