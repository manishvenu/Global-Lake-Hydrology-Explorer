# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

# Make the GLHE package importable from docs/source/
sys.path.insert(0, os.path.abspath("../.."))

# -- Project information -----------------------------------------------------

project = "Global Lake Hydrology Explorer"
copyright = "2023, Manish Venumuddula"
author = "Manish Venumuddula"
release = "0.1"

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.githubpages",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = []

# Mock heavy C-extension / environment-specific packages so autodoc can import
# GLHE modules without needing a full conda environment in CI.
autodoc_mock_imports = [
    "pandas",
    "xarray",
    "osgeo",
    "gdal",
    "matplotlib",
    "pint",
    "pint_xarray",
    "rasterio",
    "h5py",
    "cdsapi",
    "geopandas",
    "boto3",
    "botocore",
    "pyproj",
    "fsspec",
    "s3fs",
    "netCDF4",
    "dask",
    "zarr",
    "holoviews",
    "flask",
    "dash",
    "plotly",
    "dash_bootstrap_components",
    "dash_core_components",
    "dash_html_components",
    "dash_table",
    "raster2xyz",
    "reverse_geocode",
    "snakemd",
    "folium",
    "shapely",
    "requests",
]

# -- Options for HTML output -------------------------------------------------

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
