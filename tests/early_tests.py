import os

import numpy as np
import pandas as pd
import xarray as xr

from GLHE import helpers, lake_extraction, combined_data_functions
from GLHE.data_access import ERA5_Land


def test_era5_land():
    era5_dataset = ERA5_Land.get_total_precip_runoff_evap_in_sub_region(1.1, 1.2, 0.5, 0.6)
    print(era5_dataset)


def get_sample_data() -> xr.Dataset:
    s = xr.load_dataset(
        r'C:\Users\manis\OneDrive - Umich\Documents\Global Lake Hydrology '
        r'Explorer\tests\DummyData\agg_terraclimate_ppt_1958_CurrentYear_GLOBE.nc')
    s = helpers.label_xarray_dataset_with_product_name(s, "TerraClimateDummyData")
    return s


def get_sample_data_2() -> xr.Dataset:
    dims = ('time', 'lat', 'lon')
    time = pd.date_range("2021-01-01T00", "2023-12-31T23", freq="H")
    lat = [0, 1]
    lon = [0, 1]
    coords = (time, lat, lon)
    ds = xr.DataArray(data=np.random.randn(len(time), len(lat), len(lon)), coords=coords, dims=dims).rename("my_var")
    return ds.to_dataset()

def test_unit_conversion():
    dataset = get_sample_data()
    dataset = helpers.convert_units(dataset, "ppt", "in")
    print(dataset)
    return
def test_ERA_api():
    if not os.path.exists(".temp"):
        os.mkdir(".temp")
    dataset = ERA5_Land.get_total_precip_runoff_evap_in_sub_region(0.1, 0.2, 0.1, 0.2)

def test_ERA_total():
    dataset = ERA5_Land.get_ERA5_total()
    print(dataset)
def test_lake_extraction():
    dataset = get_sample_data()
    polygon = lake_extraction.extract_lake(798)
    r = lake_extraction.subset_box(dataset,polygon,1)
    terraclimate_dummy = (helpers.spatially_average_dataset(r,"ppt"))
    print("Units:",r.variables['ppt'].attrs['units'])

def test_plotting():

    #read in the data
    dataset = pd.read_csv(r'C:\Users\manis\OneDrive - Umich\Documents\Global Lake Hydrology Explorer\GLHE\.temp\test.csv')
    #convert the date column to a datetime object
    dataset['date'] = pd.to_datetime(dataset['date'])
    #set the date column as the index
    dataset.set_index('date', inplace=True)
    #read into plotting function
    combined_data_functions.plot_all_data(dataset)
    return
def main():
    os.chdir(r'C:\Users\manis\OneDrive - Umich\Documents\Global Lake Hydrology Explorer\GLHE')
    test_unit_conversion()
    helpers.clean_up_temporary_files()
    print("Ran Main")

if __name__ == "__main__":
    main()
