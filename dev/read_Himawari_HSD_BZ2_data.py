#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 17:15:33 2022

@author: ghiggi
"""
#### Read numpy arrays from a bz2/gzip file via S3
# - https://github.com/numpy/numpy/issues/18760
# - https://github.com/numpy/numpy/issues/1103

urls = ['https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B01_JP01_R10_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B02_JP01_R10_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B03_JP01_R05_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B04_JP01_R10_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B05_JP01_R20_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B06_JP01_R20_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B07_JP01_R20_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B08_JP01_R20_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B09_JP01_R20_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B10_JP01_R20_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B11_JP01_R20_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B12_JP01_R20_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B13_JP01_R20_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B14_JP01_R20_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B15_JP01_R20_S0101.DAT.bz2',
        'https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-Japan/2021/02/11/0140/HS_H08_20210211_0140_B16_JP01_R20_S0101.DAT.bz2']

# wrap urls in something that unzip, download and return byteIO --> look at bravo code 

scn = Scene(reader='ahi_hsd', filenames=urls)

#### BZ2 zipped file 
import s3fs
import numpy as np
from io import BytesIO
from bz2 import BZ2File

# THIS DOES NOT WORK 
import numpy as np
fs = s3fs.S3FileSystem(anon=True)
filename = 'HS_H08_20210409_0800_B01_FLDK_R10_S0110.DAT.bz2'
to_get = 's3://noaa-himawari8/AHI-L1b-FLDK/2021/04/09/0800/' + filename
with fs.open(to_get, compression='bz2', anon=True) as fd:
  np.fromfile(fd, dtype='u1', count=1) 
  
# THIS WORK 
# -- fromfile it doesn't work but with frombuffer it works fine
# fromfile is for binary data 
# - np.frombuffer works because you pass it a string, which is read in python by the s3fs wrapper.
# - np.fromfile and np.memmap calls low-level C routines that totally ignore the s3fs wrapper for reading --> which BUGS 
# -  np.fromfile is very low-level. It only works with objects that can be considered FILE objects on the operating system level.

# - Use fromstring with StringIO in order to read from gzip files  (or frombuffer? TODO)
# - WORKAROUND: np.frombuffer(file_object.read()) when the file is zipped
#  np.frombuffer(file_object.read(size=dtype.itemsize))
)
# Basic information block:
_BASIC_INFO_TYPE = np.dtype([("hblock_number", "u1"),
                             ("blocklength", "<u2"),
                             ("total_number_of_hblocks", "<u2"),
                             ("byte_order", "u1"),
                             ("satellite", "S16"),
                             ("proc_center_name", "S16"),
                             ("observation_area", "S4"),
                             ("other_observation_info", "S2"),
                             ("observation_timeline", "<u2"),
                             ("observation_start_time", "f8"),
                             ("observation_end_time", "f8"),
                             ("file_creation_time", "f8"),
                             ("total_header_length", "<u4"),
                             ("total_data_length", "<u4"),
                             ("quality_flag1", "u1"),
                             ("quality_flag2", "u1"),
                             ("quality_flag3", "u1"),
                             ("quality_flag4", "u1"),
                             ("file_format_version", "S32"),
                             ("file_name", "S128"),
                             ("spare", "S40"),
                             ])
with fs.open(to_get) as fd:
  bz2file = BZ2File(BytesIO(fd.read()))
  basic_info = np.frombuffer(bz2file.read(), dtype=_BASIC_INFO_TYPE, count=1)

print(basic_info)
  
# Data information block
_DATA_INFO_TYPE = np.dtype([("hblock_number", "u1"),
                            ("blocklength", "<u2"),
                            ("number_of_bits_per_pixel", "<u2"),
                            ("number_of_columns", "<u2"),
                            ("number_of_lines", "<u2"),
                            ("compression_flag_for_data", "u1"),
                            ("spare", "S40"),
                            ])

with fs.open(to_get) as fd:
  bz2file = BZ2File(BytesIO(fd.read()))
  data_info = np.frombuffer(bz2file.read(), dtype=_DATA_INFO_TYPE, count=1)
  
print(data_info)

# Projection information block
# See footnote 2; LRIT/HRIT Global Specification Section 4.4, CGMS, 1999)
_PROJ_INFO_TYPE = np.dtype([("hblock_number", "u1"),
                            ("blocklength", "<u2"),
                            ("sub_lon", "f8"),
                            ("CFAC", "<u4"),
                            ("LFAC", "<u4"),
                            ("COFF", "f4"),
                            ("LOFF", "f4"),
                            ("distance_from_earth_center", "f8"),
                            ("earth_equatorial_radius", "f8"),
                            ("earth_polar_radius", "f8"),
                            ("req2_rpol2_req2", "f8"),
                            ("rpol2_req2", "f8"),
                            ("req2_rpol2", "f8"),
                            ("coeff_for_sd", "f8"),
                            # Note: processing center use only:
                            ("resampling_types", "<i2"),
                            # Note: processing center use only:
                            ("resampling_size", "<i2"),
                            ("spare", "S40"),
                            ])
with fs.open(to_get) as fd:
  bz2file = BZ2File(BytesIO(fd.read()))
  proj_info = np.frombuffer(bz2file.read(), dtype=_PROJ_INFO_TYPE, count=1)
  
print(proj_info)

# # Navigation information block
_NAV_INFO_TYPE = np.dtype([("hblock_number", "u1"),
                           ("blocklength", "<u2"),
                           ("navigation_info_time", "f8"),
                           ("SSP_longitude", "f8"),
                           ("SSP_latitude", "f8"),
                           ("distance_earth_center_to_satellite", "f8"),
                           ("nadir_longitude", "f8"),
                           ("nadir_latitude", "f8"),
                           ("sun_position", "f8", (3,)),
                           ("moon_position", "f8", (3,)),
                           ("spare", "S40"),
                           ])
with fs.open(to_get) as fd:
  bz2file = BZ2File(BytesIO(fd.read()))
  nav_info = np.frombuffer(bz2file.read(), dtype=_NAV_INFO_TYPE, count=1)
  
print(nav_info)