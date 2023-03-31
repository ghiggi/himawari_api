# # Welcome to HIMAWARI API - An API to download and search HIMAWARI satellite data.
[![DOI](https://zenodo.org/badge/286664485.svg)](https://zenodo.org/badge/latestdoi/XXXX)
[![PyPI version](https://badge.fury.io/py/himawari_api.svg)](https://badge.fury.io/py/himawari_api)
[![Conda Version](https://img.shields.io/conda/vn/conda-forge/himawari_api.svg)](https://anaconda.org/conda-forge/goes_api)
[![Build Status](https://github.com/ghiggi/himawari_api/workflows/Continuous%20Integration/badge.svg?branch=main)](https://github.com/ghiggi/himawari_api/actions)
[![Coverage Status](https://coveralls.io/repos/github/ghiggi/himawari_api/badge.svg?branch=main)](https://coveralls.io/github/ghiggi/himawari_api?branch=main)
[![Documentation Status](https://readthedocs.org/projects/himawari_api/badge/?version=latest)](https://gpm_api.readthedocs.io/projects/himawari_api/en/stable/?badge=stable)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![License](https://img.shields.io/github/license/ghiggi/himawari_api)](https://github.com/ghiggi/himawari_api/blob/master/LICENSE)

The HIMAWARI-API is still in development. Feel free to try it out and to report issues or to suggest changes.

## Quickstart

The code in this repository provides an API to download, query and filter HIMAWARI-8 and HIMAWARI-9 satellite data.

Data download and query/filtering is available:
- for local file systems and AWS S3.
- for sensors AHI.

The folder `tutorials` provide the following jupyter notebooks, describing various features of `HIMAWARI_API`:

- Downloading HIMAWARI data: [`download.ipynb`]
- Find and filter HIMAWARI data: [`find_and_filter.ipynb`]
- Read AHI and plot it with satpy: [`read_ahi_data.ipynb`]
 
[`download.ipynb`]: https://github.com/ghiggi/himawari_api/blob/main/tutorials/00_download_and_find_files.py
[`find_and_filter.ipynb`]: https://github.com/ghiggi/himawari_api/blob/main/tutorials/01_find_utility.py
[`read_ahi_data.ipynb`]: https://github.com/ghiggi/himawari_api/blob/main/tutorials/03_read_ahi_rad_data_with_satpy.py

Documentation is available at XXXXX

## Installation

### pip

HIMAWARI-API can be installed via [pip][pip_link] on Linux, Mac, and Windows.
On Windows you can install [WinPython][winpy_link] to get Python and pip running.

Then, install the HIMAWARI-API package by typing the following command in the command terminal:

    pip install himawari_api

## Citation

If you are using HIMAWARI-API, please cite:

> Ghiggi Gionata. ghiggi/himawari_api. Zenodo. https://doi.org/10.5281/zenodo.7787851

If you want to cite a specific version, have a look at the [Zenodo site](https://doi.org/10.5281/zenodo.7787851).

## Contributors

* [Gionata Ghiggi](https://people.epfl.ch/gionata.ghiggi)

## License

The content of this repository is released under the terms of the [MIT](LICENSE) license.

[pip_link]: https://pypi.org/project/gstools
[winpy_link]: https://winpython.github.io/
