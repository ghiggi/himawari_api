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
# himawari_api. If not, see <http://www.gnu.org/licenses/>.
"""Define himawari_api exploratory and documentation tools."""

import os
import webbrowser
from .checks import (
    _check_satellite,
    _check_channel,
)


def open_directory_explorer(satellite, protocol=None, base_dir=None):
    """Open the cloud bucket / local explorer into a webpage.

    Parameters
    ----------
    base_dir : str
        Base directory path where the <HIMAWARI-**> satellite is located.
        This argument must be specified only if wanting to explore the local storage.
        If it is specified, protocol and fs_args arguments must not be specified.
    protocol : str
        String specifying the cloud bucket storage that you want to explore.
        Use `goes_api.available_protocols()` to retrieve available protocols.
        If protocol is specified, base_dir must be None !

    """
    satellite = _check_satellite(satellite)
    if protocol == "s3":
        satellite = satellite.replace("-", "")  # himawari8
        fpath = f"https://noaa-{satellite}.s3.amazonaws.com/index.html"
        webbrowser.open(fpath, new=1)
    elif base_dir is not None:
        webbrowser.open(os.path.join(base_dir, satellite))
    else:
        raise NotImplementedError("Current available protocols are 's3' and 'local'.")


def open_ahi_channel_guide(channel):
    """Open AHI QuickGuide of the channel.

    See `himawari_api.available_channels()` for available AHI channels.
    Source of information: http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/
    """
    import webbrowser

    if not isinstance(channel, str):
        raise TypeError("Expecting a string defining a single channel.")
    channel = _check_channel(channel)
    channel_number = channel[1:]  # 01-16
    url = f"http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/ABIQuickGuide_Band{channel_number}.pdf"
    webbrowser.open(url, new=1)
    return None


