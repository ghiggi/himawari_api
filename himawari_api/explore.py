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
# goes_api. If not, see <http://www.gnu.org/licenses/>.

import os
import webbrowser
from .io import (
    _check_satellite,
    _check_channel,
    _check_product,
)


def open_directory_explorer(satellite, protocol=None, base_dir=None):
    """Open the cloud bucket / local explorer into a webpage.

    Parameters
    ----------
    base_dir : str
        Base directory path where the <GOES-**> satellite is located.
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
        raise NotImplementedError(
            "Current available protocols are 'gcs', 's3', 'local'."
        )


def open_AHI_channel_guide(channel):
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


def open_AHI_L2_product_guide(product):
    """Open AHI QuickGuide of L2 products.

    See `himawari_api.available_product(sensors="AHI", product_level="L2")` for available AHI L2 products.
    Source of information: http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/
    """
    import webbrowser
    # TODO: TO UPDATE THE KEY WITH THE ACRONYM OF HIMAWARI PRODUCTS (AND DISCARD THE REST)
    dict_product_fname = {
        "ACHA": "ABIQuickGuide_BaselineCloudTopHeight.pdf",
        "ACHT": "ABIQuickGuide_BaselineCloudTopTemperature.pdf",
        "ACM": "ABIQuickGuide_BaselineClearSkyMask.pdf",
        "ACTP": "ABIQuickGuide_BaselineCloudPhase.pdf",
        "ADP": "ABIQuickGuide_BaselineAerosolDetection.pdf",
        "AICE": "JPSSQuickGuide_Ice_Concentration_2022.pdf",
        # "AITA": "Ice Thickness and Age",  # only F
        "AOD": "ABIQuickGuide_BaselineAerosolOpticalDepth.pdf",
        # "BRF": "Land Surface Bidirectional Reflectance Factor",
        # "CMIP": "Cloud and Moisture Imagery",
        "COD": "ABIQuickGuide_BaselineCloudOpticalDepth.pdf",
        "CPS": "ABIQuickGuide_BaselineCloudParticleSizeDistribution.pdf	",
        "CTP": "ABIQuickGuide_BaselineCloudTopPressure.pdf",
        "DMW": "ABIQuickGuide_BaselineDerivedMotionWinds.pdf",
        "DMWV": "ABIQuickGuide_BaselineDerivedMotionWinds.pdf",
        "DSI": "ABIQuickGuide_BaselineDerivedStabilityIndices.pdf",
        # "DSR": "Downward Shortwave Radiation",
        "FDC": "QuickGuide_GOESR_FireHotSpot_v2.pdf",
        # "LSA": "Land Surface Albedo",
        "LST": "QuickGuide_GOESR_LandSurfaceTemperature.pdf",
        "LST2KM": "QuickGuide_GOESR_LandSurfaceTemperature.pdf",
        # "LVMP": "Legacy Vertical Moisture Profile",
        # "LVTP": "Legacy Vertical Temperature Profile",
        # "MCMIP": "Cloud and Moisture Imagery (Multi-band format)",
        # "RRQPE": "Rainfall Rate (QPE)",
        # "RSR": "Reflected Shortwave Radiation at TOA",
        # "SST": "Sea Surface (Skin) Temperature",
        # "TPW": "Total Precipitable Water",
        "VAA": "QuickGuide_GOESR_VolcanicAsh.pdf",
    }
    available_products = list(dict_product_fname.keys())
    # Check product
    if not isinstance(product, str):
        raise TypeError("Expecting a string defining a single AHI L2 product.")
    product = _check_product(product=product, sensor="AHI", product_level="L2")
    # Check QuickGuide availability
    fname = dict_product_fname.get(product, None)
    if fname is None:
        raise ValueError(f"No AHI QuickGuide available for product '{product}' .\n" +
                         f"Documentation is available for the following L2 products {available_products}.")
    # Define url and open quickquide
    url = f"http://cimss.ssec.wisc.edu/goes/OCLOFactSheetPDFs/{fname}"
    webbrowser.open(url, new=1)

    return None
