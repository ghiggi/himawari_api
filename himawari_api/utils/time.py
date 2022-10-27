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
import datetime
import numpy as np 
import pandas as pd


def _dt_to_year_month_day_hhmm(dt):
    year = dt.strftime("%Y")                # year
    month = dt.strftime("%m").ljust(2,"0")  # month 
    day = dt.strftime("%d").ljust(2,"0")    # day 
    hour = dt.strftime("%H").ljust(2,"0")   # hh
    minute = int(dt.strftime("%M"))
    # Round minute down to nearest 10 multiplier 
    # - 00, 10, 20, 30, 40, 50 
    minute = int(np.floor(minute/ 10) * 10)
    minute_str = str(minute).ljust(2,"0") 
    # Define hhmm string 
    hhmm = hour + minute_str    
    return year, month, day, hhmm


def get_end_of_day(time):
    time_end_of_day = time + datetime.timedelta(days=1)
    time_end_of_day = time_end_of_day.replace(hour=0, minute=0, second=0)
    return time_end_of_day


def get_start_of_day(time):
    time_start_of_day = time
    time_start_of_day = time_start_of_day.replace(hour=0, minute=0, second=0)
    return time_start_of_day


def get_list_daily_time_blocks(start_time, end_time):
    """Return a list of (start_time, end_time) tuple of daily length."""
    # Retrieve timedelta between start_time and end_time
    dt = end_time - start_time
    # If less than a day
    if dt.days == 0:
        return [(start_time, end_time)]
    # Otherwise split into daily blocks (first and last can be shorter)
    end_of_start_time = get_end_of_day(start_time)
    start_of_end_time = get_start_of_day(end_time)
    # Define list of daily blocks
    l_steps = pd.date_range(end_of_start_time, start_of_end_time, freq="1D")
    l_steps = l_steps.to_pydatetime().tolist()
    l_steps.insert(0, start_time)
    l_steps.append(end_time)
    l_daily_blocks = [(l_steps[i], l_steps[i + 1]) for i in range(0, len(l_steps) - 1)]
    return l_daily_blocks
