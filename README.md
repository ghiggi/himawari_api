# HIMAWARI API - An API to download and query HIMAWARI satellite data.
[![DOI](https://zenodo.org/badge/286664485.svg)](https://zenodo.org/badge/latestdoi/XXXX)
[![PyPI version](https://badge.fury.io/py/himawari_api.svg)](https://badge.fury.io/py/himawari_api)
[![Conda Version](https://img.shields.io/conda/vn/conda-forge/himawari_api.svg)](https://anaconda.org/conda-forge/goes_api)
[![Build Status](https://github.com/ghiggi/himawari_api/workflows/Continuous%20Integration/badge.svg?branch=main)](https://github.com/ghiggi/himawari_api/actions)
[![Coverage Status](https://coveralls.io/repos/github/ghiggi/himawari_api/badge.svg?branch=main)](https://coveralls.io/github/ghiggi/himawari_api?branch=main)
[![Documentation Status](https://readthedocs.org/projects/himawari_api/badge/?version=latest)](https://gpm_api.readthedocs.io/projects/himawari_api/en/stable/?badge=stable)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![License](https://img.shields.io/github/license/ghiggi/himawari_api)](https://github.com/ghiggi/himawari_api/blob/master/LICENSE)


The code in this repository provides an API to download, query and filter HIMAWARI-8 and HIMAWARI-9 satellite data.

Data download and query/filtering is available:
- for local file systems and AWS S3.
- for sensors AHI.

The folder `tutorials` provide the following jupyter notebooks, describing various features of `HIMAWARI_API`:

- Downloading HIMAWARI data: [`download.ipynb`]
- Find and filter HIMAWARI data: [`find_and_filter.ipynb`]
- Read data directly from AWS S3 buckets: [`read_bucket_data.ipynb`]
- Read AHI and L2B products from cloud buckets and plot it with satpy: [`read_bucket_data_with_satpy.ipynb`]
 
[`download.ipynb`]: https://github.com/ghiggi/himawari_api/blob/main/tutorials/00_download_and_find_files.py
[`find_and_filter.ipynb`]: https://github.com/ghiggi/himawari_api/blob/main/tutorials/01_find_utility.py
[`read_bucket_data.ipynb`]: https://github.com/ghiggi/himawari_api/blob/main/tutorials/03_read_cloud_bucket_data.py
[`read_bucket_data_with_satpy.ipynb`]: https://github.com/ghiggi/himawari_api/blob/main/tutorials/03_read_cloud_bucket_data_with_satpy.py
[`kerchunk_data.ipynb`]: https://github.com/ghiggi/himawari_api/blob/main/tutorials/04_kerchunk_dataset.py

The folder `docs` contains documents with various information related to HIMAWARI AHI data products.

Documentation is available at XXXXX

## Installation

For a local installation, follow the below instructions.

1. Clone this repository.
   ```sh
   git clone git@github.com:ghiggi/himawari_api.git
   cd himawari_api
   ```

2. Install the dependencies using conda:
   ```sh
   conda env create -f environment.yml
   ```
   
3. Activate the himawari_api conda environment 
   ```sh
   conda activate himawari_api
   ```

4. Alternatively install manually the required packages with 
   ```sh
   conda install -c conda-forge numpy pandas trollsift fsspec s3fs ujson tqdm
   ```
 
## Contributors

* [Gionata Ghiggi](https://people.epfl.ch/gionata.ghiggi)

## License

The content of this repository is released under the terms of the [GNU General Public License v3.0](LICENSE.txt).
