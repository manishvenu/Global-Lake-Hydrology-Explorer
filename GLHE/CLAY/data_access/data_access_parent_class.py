import logging
import os
from abc import ABC, abstractmethod
import importlib
import GLHE.CLAY.globals
from GLHE.CALCITE import events
from GLHE.CLAY.helpers import MVSeries
from GLHE.CALCITE import pubsub, events


class DataAccess(ABC):
    """Parent class for all data access. This class is an abstract class and should not be instantiated."""

    logger: logging.Logger
    README_default_information: str

    @abstractmethod
    def __init__(self):
        """Initializes the data access class"""
        self.create_logger()
        if not self.verify_inputs():
            raise ValueError("Invalid inputs")
        self.logger.info("***Initialized: " + self.__class__.__name__ + "***")
        pass

    def create_logger(self) -> None:
        """Creates a logger for the child classes"""
        self.logger = logging.getLogger(self.__class__.__name__)

        fh = logging.FileHandler(
            os.path.join(
                GLHE.CLAY.globals.config["DIRECTORIES"]["LOGGING_DIRECTORY"],
                self.__class__.__name__ + "_driver.log",
            )
        )
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s.%(msecs)03d %(levelname)s: %(module)s.%(funcName)s: %(message)s"
        )
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    @abstractmethod
    def verify_inputs(self) -> bool:
        """
        Verifies that all inputs are valid. This function should be called before any other functions in this class.
        Returns
        -------
        bool
            True if all inputs are valid, False otherwise
        """
        self.logger.info("Verifying inputs: " + self.__class__.__name__)
        pass

    def send_data_product_event(self, msg) -> None:
        """
        Sends data product event to event bus
        """
        pubsub.EventBus.Publish(msg)

    @abstractmethod
    def attach_geodata(self) -> str:
        """
        Attaches to the output a file containing the geodata for the data product, If "grid" is returned, the driver processes the attachment later on.
        Returns
        -------
        bool
            True if all inputs are valid, False otherwise
        """
        self.logger.info("Attaching Geo Data inputs: " + self.__class__.__name__)
        pass

    @abstractmethod
    def product_driver(self, polygon, debug=False, run_cleanly=False) -> list[MVSeries]:
        """
        Returns data for a given lake using functions from helpers and data_access
        Parameters
        ----------
        polygon : shapely.geometry.Polygon
            The polygon of the lake
        debug : bool
            If True, the function will use a cached version of the data if it exists. If False, the function will
            download the data from real data.
        run_cleanly : bool
            If True, this function will run the data collection from the start.
        Returns
        -------
        list[MVSeries]
            List of MVSeries objects containing the data
        """
        pubsub.EventBus.Publish(events.topics["data_product_run_event"])
        pass
