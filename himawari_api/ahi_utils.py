#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 26 11:57:01 2022

@author: ghiggi
"""
# TODO ADAPT
def get_scan_mode_from_attrs(attrs):
    timeline_id = attrs['timeline_id']
    if timeline_id == "AHI Mode 3":
        scan_mode = "M3"
    elif timeline_id == "AHI Mode 4":
        scan_mode = "M4"
    elif timeline_id == "AHI Mode 6":
        scan_mode = "M6"
    else: 
        raise ValueError(f"'timeline_id' attribute not recognized. Value is {timeline_id}.")
    return scan_mode 

def get_sector_from_attrs(attrs):
    scene_id = attrs['scene_id']
    if scene_id == "Full Disk":
        sector = "F"
    elif scene_id == "CONUS": # TO ADAPT
        sector = "C"
    elif scene_id == "Mesoscale":
        sector = "M"
    else: 
        raise ValueError(f"'scene_id' attribute not recognized. Value is {scene_id}.")
    return sector 

def get_resolution_from_attrs(attrs):
    spatial_resolution = attrs['spatial_resolution']
    if spatial_resolution == "0.5km at nadir":
        resolution = "500"
    elif spatial_resolution == "1km at nadir":
        resolution = "1000"
    elif spatial_resolution == "2km at nadir":
        resolution = "2000"
    elif spatial_resolution == "4km at nadir":
        resolution = "4000"
    elif spatial_resolution == "8km at nadir":
        resolution = "8000"
    elif spatial_resolution == "10km at nadir":
        resolution = "10000"
    else: 
        raise ValueError(f"'spatial_resolution' attribute not recognized. Value is {spatial_resolution}.")
    return resolution 

# TODO: ADATP SHAPE 
def get_AHI_shape(sector, resolution): 
    resolution = int(resolution) 
    # Retrieve shape at 500 m 
    if sector == "F": 
        shape = (21696, 21696)
    elif sector == "C":
        shape = (6000, 10000)
    else: # sector == "M"
        shape = (2000, 2000)
    # Retrieve shape at reduced resolutions 
    reduction_factor = int(resolution/500)
    shape = tuple([int(pixels/reduction_factor) for pixels in shape])
    return shape 