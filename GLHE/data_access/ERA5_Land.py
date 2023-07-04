import logging
import asyncio
import cdsapi
import xarray as xr
from GLHE.data_access import data_access_parent_class
from GLHE.helpers import MVSeries
from GLHE import helpers, xarray_helpers, lake_extraction


class ERA5_Land(data_access_parent_class.DataAccess):
    api_client: cdsapi.Client
    xarray_dataset: xr.Dataset

    def __init__(self):
        """Initializes the ERA5 Land Data Access class"""

        self.api_client = cdsapi.Client()
        self.xarray_dataset = None
        super().__init__()

    def verify_inputs(self) -> bool:
        """See parent class for description"""
        self.logger.info("Verifying inputs: " + self.__class__.__name__)
        return True

    async def get_total_precip_runoff_evap_in_subset_box_api(self, west: float, east: float, south: float,
                                                             north: float) -> xr.Dataset:
        """Gets ERA5 Land precip, evap, & runoff data in a known subregion by lat-long

        Parameters
        ----------
        west : str
            west direction(-180,180), must be less than east
        east : str
            east direction(-180,180), must be greater than west
        south : str
            south direction(-90,90), must be less than north
        north : float
            north direction(-90,90), must be greater than south

        Returns
        -------
        xarray Dataset
            xarray Dataset format of the evap, precip, & runoff in a grid
        """

        try:
            self.api_client.retrieve(
                'reanalysis-era5-land-monthly-means',
                {
                    'product_type': 'monthly_averaged_reanalysis',
                    'variable': [
                        'runoff', 'total_evaporation', 'total_precipitation',
                    ],
                    'year': [
                        '1950', '1951', '1952',
                        '1953', '1954', '1955',
                        '1956', '1957', '1958',
                        '1959', '1960', '1961',
                        '1962', '1963', '1964',
                        '1965', '1966', '1967',
                        '1968', '1969', '1970',
                        '1971', '1972', '1973',
                        '1974', '1975', '1976',
                        '1977', '1978', '1979',
                        '1980', '1981', '1982',
                        '1983', '1984', '1985',
                        '1986', '1987', '1988',
                        '1989', '1990', '1991',
                        '1992', '1993', '1994',
                        '1995', '1996', '1997',
                        '1998', '1999', '2000',
                        '2001', '2002', '2003',
                        '2004', '2005', '2006',
                        '2007', '2008', '2009',
                        '2010', '2011', '2012',
                        '2013', '2014', '2015',
                        '2016', '2017', '2018',
                        '2019', '2020', '2021',
                        '2022',
                    ],
                    'month': [
                        '01', '02', '03',
                        '04', '05', '06',
                        '07', '08', '09',
                        '10', '11', '12',
                    ],
                    'time': '00:00',
                    'area': [
                        north, west, south,
                        east,
                    ],
                    'format': 'netcdf',
                },
                ".temp/TEMPORARY_ERA5Land_DONOTOPEN_ERA5LAND.nc")
        except:
            self.logger.info("Error calling API")
            return None
        self.logger.info("Success calling ERA5 Land API")
        xarray_dataset = xr.open_dataset(".temp/TEMPORARY_ERA5Land_DONOTOPEN_ERA5LAND.nc")
        return xarray_dataset

    async def product_driver(self, polygon, debug=False) -> list[MVSeries]:
        """See parent function for details"""
        self.logger.info("ERA Driver Started: Precip & Evap")
        if not debug:
            min_lon, min_lat, max_lon, max_lat = polygon.bounds

            # Need to add more padding to lat, longs ##
            min_lon = min_lon - 1
            max_lon = max_lon + 1
            min_lat = min_lat - 1
            max_lat = max_lat + 1

            dataset = await self.get_total_precip_runoff_evap_in_subset_box_api(min_lon, max_lon, min_lat, max_lat)
            dataset = xarray_helpers.label_xarray_dataset_with_product_name(dataset, "ERA5_Land")
            dataset = xarray_helpers.fix_lat_long_names_in_xarray_dataset(dataset)
            try:
                dataset = lake_extraction.subset_box(dataset, polygon, 1)
            except ValueError:
                self.logger.error(
                    "ERA5 Land subset Failed: Polygon is too small for ERA5 Land, trying again with larger polygon")
                dataset = lake_extraction.subset_box(dataset, polygon.buffer(0.5), 0)
            dataset = xarray_helpers.fix_weird_units_descriptors_in_xarray_datasets(dataset, "e", "m")
            dataset = xarray_helpers.add_descriptive_time_component_to_units_in_xarray_dataset(dataset, "day")
            dataset = xarray_helpers.convert_xarray_dataset_units(dataset, "mm/month", "tp", "e")
            dataset = xarray_helpers.make_sure_xarray_dataset_is_positive(dataset, "e")
            helpers.pickle_var(dataset, dataset.attrs['product_name'])
        else:
            self.logger.info("Debug mode, using pickled ERA5 Land data")
            dataset = helpers.unpickle_var("ERA5_Land")
        evap_ds, precip_ds = xarray_helpers.spatially_average_xarray_dataset_and_convert(dataset, "e", "tp")
        helpers.clean_up_specific_temporary_files("ERA5Land")
        list_of_MVSeries = [precip_ds, evap_ds]
        self.logger.info("ERA Driver Finished")
        return list_of_MVSeries


if __name__ == "__main__":
    print("This is the ERA5 module, not a script")
