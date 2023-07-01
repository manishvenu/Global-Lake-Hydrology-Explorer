from abc import ABC, abstractmethod
from GLHE.helpers import MVSeries
import logging


class DataAccess(ABC):
    """Parent class for all data access. This class is an abstract class and should not be instantiated."""
    logger = logging.getLogger(__name__)

    @abstractmethod
    def __init__(self):
        """Initializes the data access class"""
        if not self.verify_inputs():
            raise ValueError("Invalid inputs")
        pass

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

    @abstractmethod
    def product_driver(self, polygon, debug=False) -> list[MVSeries]:
        """
        Returns data for a given lake using functions from helpers and data_access
        Parameters
        ----------
        polygon : shapely.geometry.Polygon
            The polygon of the lake
        debug : bool
            If True, the function will use a cached version of the data if it exists. If False, the function will
            download the data from real data.
        Returns
        -------
        list[MVSeries]
            List of MVSeries objects containing the data
        """
        pass
