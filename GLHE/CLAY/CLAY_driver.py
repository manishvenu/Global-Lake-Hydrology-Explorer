import json
import logging
import os
import sys

import pandas as pd
import GLHE.CLAY.globals
from GLHE.CLAY import combined_data_functions, events, lake_extraction, helpers
from GLHE.CLAY.data_access import data_check, ERA5_Land, CRUTS, NWM
from pathlib import Path


class CLAY_driver:
    """
    This is the driver class.
    """
    datasets_index = {
        "all": [],
        "grid": [],
        "slc": {}
    }
    pandas_dataset: pd.DataFrame
    root_logger: logging.Logger
    data_products = {}
    read_me_information = {"Data_Product": {}, "Output_File": {}}
    output_file_config = {}
    lake_polygon = None

    def __init__(self):
        """
        This is the constructor for the driver class.
        """
        self.datasets_index = {
            "all": [],
            "grid": [],
            "slc": {}
        }
        for key in GLHE.CLAY.globals.SLC_MAPPING_REVERSE_NAMES:
            self.datasets_index["slc"][key] = []
        self.pandas_dataset = None
        self.root_logger = None
        self.data_products = {}
        self.read_me_information = {"Data_Product": {}, "Output_File": {}}
        self.output_file_config = {}
        self.lake_polygon = None
        pub.subscribe(self.output_file_listener, events.topics["output_file_event"])
        pub.subscribe(self.read_me_data_product_run_listener, events.topics["data_product_run_event"])

    def index_datasets(self, *datasets: helpers.MVSeries):
        """
        This function indexes the datasets by their SLCs.
        """
        for key in GLHE.CLAY.globals.SLC_MAPPING_REVERSE_NAMES:
            self.datasets_index["slc"][key] = []
        for ds in self.datasets_index["all"]:
            # By Slc
            self.datasets_index["slc"][ds.single_letter_code].append(ds)

    def set_up_logging(self) -> None:
        helpers.setup_logging_directory(".temp")
        logging.basicConfig(
            encoding='utf-8', level=os.environ.get("LOGLEVEL", "INFO"),
            format='%(asctime)s.%(msecs)03d %(levelname)s: %(module)s.%(funcName)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S', )
        fh = logging.FileHandler(
            os.path.join(GLHE.CLAY.globals.config["DIRECTORIES"]["LOGGING_DIRECTORY"], "GLHE_root.log"))
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s: %(module)s.%(funcName)s: %(message)s')
        fh.setFormatter(formatter)
        self.root_logger = logging.getLogger()
        self.root_logger.addHandler(fh)
        # self.root_logger.addHandler(logging.StreamHandler(sys.stdout))

    def read_me_data_product_run_listener(self, message: events.DataProductRunEvent) -> None:
        """
        This function is a listener for the data product run event.
        """
        # print("Data Product Message: " + str(message))
        self.read_me_information["Data_Product"][message.product_name] = message.product_description

    def output_file_listener(self, message: events.OutputFileEvent) -> None:
        """
        This function is a listener for the output file event.
        """
        # print("Output File Message: " + str(message))
        self.read_me_information["Output_File"][message.file_name] = message.file_description
        self.output_file_config[message.LIME_file_type] = message.file_path

    def main(self, HYLAK_ID: int) -> str:
        """
        This is the driver function.
        Here, we'll run all the code.
        Returns path to output directory!
        """

        # Set up logging #
        self.set_up_logging()
        self.root_logger.info("***********************Initializing Driver Function*************************")

        if GLHE.CLAY.globals.config["DEBUG"]:
            self.root_logger.info("Debug Mode is On")

        # Verify access to data #
        data_check.check_data_and_download_missing_data_or_files()
        # Access lake information#
        lake_extraction_object = lake_extraction.LakeExtraction()
        lake_extraction_object.extract_lake_information(HYLAK_ID)
        GLHE.CLAY.globals.config["LAKE_NAME"] = lake_extraction_object.get_lake_name()

        helpers.setup_output_directory(GLHE.CLAY.globals.config["LAKE_NAME"])

        # Transition to new logging directory
        helpers.setup_logging_directory(
            os.path.join(GLHE.CLAY.globals.config["DIRECTORIES"]["LAKE_OUTPUT_FOLDER"],
                         GLHE.CLAY.globals.config["LAKE_NAME"]))

        # Get lake polygon #
        self.lake_polygon = lake_extraction_object.get_lake_polygon()

        # Collect the data #
        self.data_products = self.load_data_product_list()

        for key in self.data_products:
            if self.data_products[key]["run_on_start"]:
                self.data_products[key]["loaded"] = True
                try:
                    self.data_products[key]["output_datasets"] = (
                        self.data_products[key]["object"].product_driver(self.lake_polygon,
                                                                         GLHE.CLAY.globals.config["DEBUG"],
                                                                         GLHE.CLAY.globals.config["RUN_CLEANLY"]))
                    for ds in self.data_products[key]["output_datasets"]:
                        self.root_logger.info(ds)
                    self.datasets_index["all"].extend(self.data_products[key]["output_datasets"])
                except Exception as e:
                    self.data_products[key]["loaded"] = False
                    self.root_logger.error(
                        "Data Product: {} not available for this lake with Exception: {}".format(key, e))

        # Attach Geodata
        for key in self.data_products:
            if self.data_products[key]["loaded"]:
                response = self.data_products[key]["object"].attach_geodata()
                if response == "grid":
                    self.datasets_index["grid"].extend(self.data_products[key]["output_datasets"])
                elif response == "complete":
                    pass
                else:
                    raise ValueError("Response from attach_geodata() must be either 'grid' or 'complete'")

        combined_data_functions.present_mv_series_as_geospatial_at_date_time(pd.to_datetime('2002-05-01'),
                                                                             *self.datasets_index["grid"])

        # Plot and Output
        self.index_datasets()  # Required for datasets_index: slc
        self.pandas_dataset = combined_data_functions.merge_mv_series_into_pandas_dataframe(self.datasets_index["slc"])
        combined_data_functions.output_plot_of_all_data(self.pandas_dataset)
        combined_data_functions.output_all_compiled_data_to_csv(GLHE.CLAY.globals.config["LAKE_NAME"] + "_Data.csv",
                                                                self.pandas_dataset)

        self.export_data_product_config()
        combined_data_functions.write_and_output_README(self.read_me_information)
        combined_data_functions.write_and_output_LIME_CONFIG(self.output_file_config)
        logging.info('"***********************Finished Driver Function*************************"')
        return GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"]

    def export_data_product_config(self) -> None:
        data_products_temp = self.data_products.copy()
        for key in self.data_products:
            data_products_temp[key]["object"] = None
            data_products_temp[key]["output_datasets"] = None
        output_file_name = GLHE.CLAY.globals.config["DIRECTORIES"]["OUTPUT_DIRECTORY"] + "/" + GLHE.CLAY.globals.config[
            "LAKE_NAME"] + "_data_products.json"
        with open(output_file_name, 'w') as f:
            json.dump(data_products_temp, f, indent=4)
        pub.sendMessage(events.topics["output_file_event"],
                        message=events.OutputFileEvent(output_file_name, output_file_name, ".json",
                                                       "The products considered by GLHE",
                                                       events.TypeOfFileLIME.BLEEPBLEEP))

    def run_product(self, key: str) -> None:
        """
        This function runs the specified product from self.data_products
        """
        logging.info('"***********************Start Product Run*************************"')
        if key is None:
            return
        self.data_products[key]["object"] = getattr(globals()[self.data_products[key]["module_name"]],
                                                    self.data_products[key]["class_name"])()
        self.data_products[key]["output_datasets"] = []
        try:
            self.data_products[key]["loaded"] = True
            self.data_products[key]["output_datasets"] = (
                self.data_products[key]["object"].product_driver(self.lake_polygon,
                                                                 GLHE.CLAY.globals.config["DEBUG"],
                                                                 GLHE.CLAY.globals.config["RUN_CLEANLY"]))
            for ds in self.data_products[key]["output_datasets"]:
                self.root_logger.info(ds)
            self.datasets_index["all"].extend(self.data_products[key]["output_datasets"])
        except Exception as e:
            self.data_products[key]["loaded"] = False
            self.root_logger.error(
                "Data Product: {} not available for this lake with Exception: {}".format(key, e))

        # Attach Geodata
        response = self.data_products[key]["object"].attach_geodata()
        if response == "grid":
            self.datasets_index["grid"].extend(self.data_products[key]["output_datasets"])
        elif response == "complete":
            pass
        else:
            raise ValueError("Response from attach_geodata() must be either 'grid' or 'complete'")

        combined_data_functions.present_mv_series_as_geospatial_at_date_time(pd.to_datetime('2002-05-01'),
                                                                             *self.datasets_index["grid"])

        # Plot and Output
        self.index_datasets()  # Required for datasets_index: slc
        self.pandas_dataset = combined_data_functions.merge_mv_series_into_pandas_dataframe(self.datasets_index["slc"])
        combined_data_functions.output_plot_of_all_data(self.pandas_dataset)
        combined_data_functions.output_all_compiled_data_to_csv(GLHE.CLAY.globals.config["LAKE_NAME"] + "_Data.csv",
                                                                self.pandas_dataset)
        self.export_data_product_config()
        combined_data_functions.write_and_output_README(self.read_me_information)
        combined_data_functions.write_and_output_LIME_CONFIG(self.output_file_config)

        logging.info('"***********************Finished Product Run*************************"')

    def load_data_product_list(self) -> dict:
        """
        This function loads the data product list, and adds required fields.
        """
        with open(os.path.join(Path(__file__).parent, "config", "data_products.json"), 'r') as f:
            self.data_products = json.load(f)
        for key in self.data_products:
            self.data_products[key]["object"] = getattr(globals()[self.data_products[key]["module_name"]],
                                                        self.data_products[key]["class_name"])()
            self.data_products[key]["output_datasets"] = []

        return self.data_products


if __name__ == "__main__":
    CLAY_driver_obj = CLAY_driver()
    HYLAK_ID = 67
    CLAY_driver_obj.main(HYLAK_ID)
