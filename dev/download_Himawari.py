#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 14 16:16:35 2020

@author: ghiggi
"""
# # https://noaa-himawari8.s3.amazonaws.com/index.html


## Inspired from https://github.com/zxdawn/weather_data/tree/master/H8
# Xin Zhang

# FTP: ftp://ftp.ptree.jaxa.jp/

# ftp://hmwr829gr.cr.chiba-u.ac.jp/gridded/FD/V20190123/
## Remote directory
# /pub/himawari
#     +---/L2
#         +---/[PRODUCT]
#             +---/[VER]
#                 +---/[YYYYMM]
#                     +---/[DD]
#                         +---/[hh]





# Options:
#   -s, --save_path TEXT         Directory where you want to save files.
#                                [default: ./data/H8/]

#   -sd, --sdate TEXT            Beginning date of downloaded files YYYY-MM-
#                                DD_hh:mm

#   -ed, --edate TEXT            Ending date of downloaded files YYYY-MM-
#                                DD_hh:mm

#   -ts, --tstep INTEGER         Time step (min) between files  [default: 10]
#   -p, --product [ARP|CLP|PAR]  Product name in Capital. ARP, CLP or PAR
#                                [default: CLP]

#   -u, --username TEXT          Username
#   -pwd, --password TEXT        Password
#   -d, --debug INTEGER          Debug level  [default: 0]
#   --help                       Show this message and exit.

## python h8_l2.py -p CLP -sd <YYYY-MM-DD_HH:MM> -ed <YYYY-MM-DD_HH:MM> -u <username> -pwd <password>




import os
import sys
import click
import ftplib
import ntpath
from tqdm import tqdm
from datetime import datetime
from datetime import timedelta


def files_list(d1, d2, tstep, product):
    '''
    Generate filenames by time step
    https://stackoverflow.com/questions/
        39298054/generating-15-minute-time-interval-array-in-python
    '''
    files = []
    step = timedelta(minutes=tstep)
    seconds = (d2-d1).total_seconds()

    # generate basenames
    for i in range(0, int(seconds), int(step.total_seconds())):
        files.append(d1 + timedelta(seconds=i))

    # get all files from sdate to edate by tstep
    files = [date.strftime('%Y%m/%d/%H/') +
             f'*H08_{date.strftime("%Y%m%d_%H%M")}_*_FLDK*nc'
             for date in files]

    return files


def downloadFiles(ftp, source, product, file, destination, debug):
    '''
    Download files of newest version
    '''
    # omit 'bet' version
    files = [os.path.basename(f) for f in ftp.nlst(source+product) if os.path.basename(f).isdigit()]
    # get the newest version
    version = sorted(files, key=lambda x: float(x))[-1]

    try:
        ftp.cwd(os.path.dirname(source+product+'/'+version+'/'+file))
    except OSError:
        pass
    except ftplib.error_perm:
        print('Error: could not change to ' + os.path.dirname(source+product+'/'+version+'/'+file))
        return 0

    filename = ntpath.basename(file)
    try:
        filename = ftp.nlst(filename)[0]
        ftp.sendcmd('TYPE I')
        filesize = ftp.size(filename)

        # create directory
        if not os.path.exists(os.path.dirname(destination)):
            os.makedirs(os.path.dirname(destination))

        # download data
        with open(os.path.dirname(destination)+'/'+filename, 'wb') as f:
            # set progress bar
            with tqdm(total=filesize, unit_scale=True, desc=filename, miniters=1,
                      file=sys.stdout, leave=False) as pbar:
                def file_write(data):
                    pbar.update(len(data))
                    f.write(data)
                ftp.retrbinary('RETR ' + filename, file_write)

            if debug > 0:
                print('    Downloaded')

    except:
        print('Error: File could not be downloaded ' + filename)
        return 0

    return 1


# -------------------------------------------------------
@click.command()
@click.option(
    '--save_path',
    '-s',
    default='./data/H8/',
    help='Directory where you want to save files.',
    show_default=True
)

@click.option(
    '--sdate',
    '-sd',
    help='''
            Beginning date of downloaded files
            YYYY-MM-DD_hh:mm
         '''
)

@click.option(
    '--edate',
    '-ed',
    help='''
            Ending date of downloaded files
            YYYY-MM-DD_hh:mm
         '''
)

@click.option(
    '--tstep',
    '-ts',
    default=10,
    help='Time step (min) between files',
    show_default=True
)

@click.option(
    '--product',
    '-p',
    type=click.Choice(['ARP', 'CLP', 'PAR']),
    default='CLP',
    help='''
            Product name in Capital.
            ARP, CLP or PAR
         ''',
    show_default=True,
)

@click.option(
    '--username',
    '-u',
    help='Username',
    show_default=True
)

@click.option(
    '--password',
    '-pwd',
    help='Password',
    show_default=True
)

@click.option(
    '--debug',
    '-d',
    default=0,
    help='Debug level',
    show_default=True
)


def main(save_path, sdate, edate, tstep,
         product, username, password, debug):
    '''
    \b
    Fuctions:
        Download Himawari-8 Level 2 products from JAXA and save to directories
    Contact:
        xinzhang1215@gmail.com
    '''
    server = 'ftp.ptree.jaxa.jp'  # JAXA data server
    source = '/pub/himawari/L2/'
    save_path = os.path.join(save_path, "")

    # get the list of datetime from sdate to edate by day
    d1 = datetime.strptime(sdate, '%Y-%m-%d_%H:%M')
    d2 = datetime.strptime(edate, '%Y-%m-%d_%H:%M')

    # get filenames based on dates
    files = files_list(d1, d2, tstep, product)

    ftp = ftplib.FTP(server)
    ftp.login(username, password)

    for file in tqdm(files, desc='total progress'):
        # iterate and download files
        filename = ntpath.basename(file)
        destination = os.path.join(save_path+file)

        if debug > 0:
            print('Downloading ' + filename + ' ...')

        file_exist = downloadFiles(ftp, source, product, file, destination, debug)

        # skip following steps if file isn't found
        if not file_exist:
            continue


if __name__ == '__main__':
    main()