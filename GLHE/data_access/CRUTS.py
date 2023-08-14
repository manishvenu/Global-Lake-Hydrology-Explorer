import logging

import xarray as xr
from GLHE.data_access import data_access_parent_class
from GLHE.helpers import MVSeries
from GLHE import xarray_helpers, helpers, lake_extraction, events
from pubsub import pub


class CRUTS(data_access_parent_class.DataAccess):
    xarray_dataset: xr.Dataset

    def __init__(self):
        """Initializes the CRUTS Land Data Access class"""

        self.xarray_dataset = None
        super().__init__()
        self.README_default_information = "Validate this data with the gridded geodata in the zip file"

    def verify_inputs(self) -> bool:
        """See parent class for description"""
        self.logger.info("Verifying inputs: " + self.__class__.__name__)
        return True

    def attach_geodata(self) -> str:
        self.logger.info("Attaching Geo Data inputs: " + self.__class__.__name__)
        return "grid"

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

    def product_driver(self, polygon, debug=False, run_cleanly=False) -> list[MVSeries]:
        """See parent class for description"""

        self.logger.info("CRUTS Driver Started: Precip & PET")

        if not run_cleanly and not debug:
            try:
                self.logger.info("Using pickled CRUTS Land data")
                dataset = helpers.unpickle_var("CRUTS")
            except FileNotFoundError:
                self.logger.info("No saved CRUTS Land data found, calling script to download and process data")
                dataset = self.call_CRUTS_access_and_process(polygon)
        else:
            dataset = self.call_CRUTS_access_and_process(polygon)

        pet_ds, precip_ds = xarray_helpers.spatially_average_xarray_dataset_and_convert(dataset, "pet", "pre")
        pet_ds, precip_ds = helpers.move_date_index_to_first_of_the_month(pet_ds, precip_ds)
        helpers.clean_up_specific_temporary_files("CRUTS")
        list_of_MVSeries = [pet_ds, precip_ds]
        self.send_data_product_event(events.DataProductRunEvent("CRUTS", self.README_default_information))
        self.logger.info("CRUTS Driver Finished")
        return list_of_MVSeries

    def call_CRUTS_access_and_process(self, polygon) -> xr.Dataset:
        """
        Calls the CRUTS local data and processes the data

        """
        dataset = self.get_total_precip_evap()
        dataset = xarray_helpers.label_xarray_dataset_with_product_name(dataset, "CRUTS")
        dataset = xarray_helpers.fix_lat_long_names_in_xarray_dataset(dataset)
        try:
            dataset = lake_extraction.subset_box(dataset, polygon, 1)
        except ValueError:
            self.logger.error("CRUTS subset Polygon is too small for CRUTS, trying again with larger polygon")
            dataset = lake_extraction.subset_box(dataset, polygon.buffer(0.5), 0)
        dataset = xarray_helpers.convert_xarray_dataset_units(dataset, "mm/month", "pet", "pre")
        helpers.pickle_var(dataset, dataset.attrs['product_name'])
        return dataset


if __name__ == "__main__":
    print("This is the CRUTS module, not a script")
