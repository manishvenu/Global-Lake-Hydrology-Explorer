from GLHE.data_access import ERA5_Land
from GLHE import helpers
import xarray as xr
import numpy as np
import pandas as pd


def test_ERA5_Land():
    era5_dataset = ERA5_Land.get_total_precip_runoff_evap_in_sub_region(1.1, 1.2, 0.5, 0.6)
    print(era5_dataset)


def get_sample_data()->xr.Dataset:
    return xr.open_dataset(
        r'C:\Users\manis\OneDrive - Umich\Documents\Global Lake Hydrology '
        r'Explorer\tests\DummyData\agg_terraclimate_ppt_1958_CurrentYear_GLOBE.nc')

def get_sample_data_2()->xr.Dataset:
    dims = ('time', 'lat', 'lon')
    time = pd.date_range("2021-01-01T00", "2023-12-31T23", freq="H")
    lat = [0, 1]
    lon = [0, 1]
    coords = (time, lat, lon)
    ds = xr.DataArray(data=np.random.randn(len(time), len(lat), len(lon)), coords=coords, dims=dims).rename("my_var")
    return ds.to_dataset()


def main():
    helpers.group_dataset_by_month(get_sample_data_2())
    print("Ran Main")


if __name__ == "__main__":
    main()
