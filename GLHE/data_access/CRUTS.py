import logging

import xarray as xr
from GLHE.data_access import data_access_parent_class
from GLHE.helpers import MVSeries
from GLHE import helpers, lake_extraction


class CRUTS(data_access_parent_class.DataAccess):
    xarray_dataset: xr.Dataset

    def __init__(self):
        """Initializes the CRUTS Land Data Access class"""

        self.xarray_dataset = None
        super().__init__()

    def verify_inputs(self) -> bool:
        """See parent class for description"""
        self.logger.info("Verifying inputs: " + self.__class__.__name__)
        return True

    def get_total_precip_evap(self) -> xr.Dataset:
        """Gets CRUTS evap

        Parameters
        ----------
        None

        Returns
        -------
        xarray Dataset
            xarray Dataset format of the evap, precip, & runoff in a grid
        """
        self.logger.info("Reading in CRUTS data from LocalData folder")
        self.xarray_dataset = xr.load_dataset("LocalData\\cruts_pet_pre_4.07_1901_2022.nc")
        return self.xarray_dataset

    def product_driver(self, polygon, debug=False) -> list[MVSeries]:
        """See parent class for description"""

        self.logger.info("CRUTS Driver Started: Precip & PET")
        if not debug:
            dataset = self.get_total_precip_evap()
            dataset = helpers.label_xarray_dataset_with_product_name(dataset, "CRUTS")
            dataset = helpers.fix_lat_long_names(dataset)
            try:
                dataset = lake_extraction.subset_box(dataset, polygon, 1)
            except ValueError:
                self.logger.error("CRUTS subset Polygon is too small for CRUTS, trying again with larger polygon")
                dataset = lake_extraction.subset_box(dataset, polygon.buffer(0.5), 0)
            dataset = helpers.convert_units(dataset, "mm/month", "pet", "pre")
            helpers.pickle_xarray_dataset(dataset)
        else:
            dataset = helpers.load_pickle_dataset("CRUTS")
        pet_ds, precip_ds = helpers.spatially_average_dataset_and_convert(dataset, "pet", "pre")
        pet_ds, precip_ds = helpers.move_date_index_to_first_of_the_month(pet_ds, precip_ds)
        helpers.clean_up_temporary_files()
        list_of_MVSeries = [pet_ds, precip_ds]
        self.logger.info("CRUTS Driver Finished")
        return list_of_MVSeries


if __name__ == "__main__":
    print("This is the CRUTS module, not a script")
