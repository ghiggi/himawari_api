#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 22:16:25 2022

@author: ghiggi
"""
from pathlib import Path
from setuptools import setup, find_packages

HERE = Path(__file__).parent
README = (HERE / "README.md").read_text(encoding="utf8")

requires = ["numpy", "pandas", "trollsift", "fsspec", "s3fs", "tqdm"]

setup(
    # Metadata
    name="himawari_api",
    version="0.0.1",
    author="Gionata Ghiggi",
    author_email="gionata.ghiggi@gmail.com",
    description="Python Package for I/O of Himawari-8/9 satellite data on local and cloud storage.",
    long_description=README,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 1 - Planning",  # https://pypi.org/classifiers/
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Natural Language :: English",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Scientific/Engineering :: Atmospheric Science",
    ],
    url="https://github.com/ghiggi/himawari_api",
    project_urls={
        "Source Code": "https://github.com/ghiggi/himawari_api",
        "Documentation": "https://ghiggi.github.io/himawari_api/_build/html/",
    },
    license="GNU",
    # Options
    packages=find_packages(),
    package_data={
        "": ["*.cfg"],
    },
    keywords=[
        "Himawari-8",
        "Himawari-9",
        "AHI",
        "satellite",
        "weather",
        "meteorology",
        "forecasting",
        "EWS",
    ],
    install_requires=requires,
    python_requires=">=3.7",
    zip_safe=False,
)

