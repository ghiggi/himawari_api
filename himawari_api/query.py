#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 11:36:43 2022

@author: ghiggi
"""
from .io import get_key_from_filepaths


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

 




