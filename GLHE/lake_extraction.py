import logging
import os
from json import loads

import xarray as xr
from osgeo.gdal import OpenEx, OF_VECTOR, UseExceptions
from shapely.geometry import shape, Polygon
import GLHE.globals

logger = logging.getLogger(__name__)


class LakeExtraction:
    """Lake extraction class, mainly to hold the geojson file."""
    logger: logging.Logger
    lake_information: str  # As a GeoJSON
    polygon: Polygon
    lake_name: str

    def __init__(self):
        """Initializes the data access class"""
        self.create_logger()
        if not self.verify_inputs():
            raise ValueError("Invalid inputs")
        lake_information = None

    def verify_inputs(self) -> bool:
        """Not Implemented Yet"""
        return True

    def create_logger(self) -> None:
        """Creates a logger"""
        self.logger = logging.getLogger(self.__class__.__name__)
        fh = logging.FileHandler(
            os.path.join(GLHE.globals.TEMP_DIRECTORY, "logging", self.__class__.__name__ + "_driver.log"))
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s: %(module)s.%(funcName)s: %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def extract_lake_information(self, hylak_id: int) -> Polygon:
        """Extracts lake from shapefile

             Parameters
             __________

             hyset_id : int
                    ID of the lake (Mono is 798)

            Returns
            _______

            shapely Polygon
                    polygon of specified lake
            """
        UseExceptions()

        # Read in the shapefile and open it with gdal.OpenEx
        hydro_lakes_shapefile_location = r'LocalData\HydroLAKES_polys_v10_shp\HydroLAKES_polys_v10.shp'
        hydro_lakes = OpenEx(hydro_lakes_shapefile_location, OF_VECTOR)

        # Access the data layer and access the lake from the lake ID passed in (It's in SQL)
        layer = hydro_lakes.GetLayer()
        query = "SELECT * FROM {} WHERE hylak_id = \'{}\'".format(layer.GetName(), hylak_id)
        result = hydro_lakes.ExecuteSQL(query)

        # Get the lake polygon and export as a geoJSON
        feature = result.GetNextFeature()
        self.lake_information = loads(feature.ExportToJson())
        self.lake_name = self.lake_information['properties']['Lake_name'].replace(" ", "_")
        logger.info("Extracted Lake: {}".format(self.lake_name))
        self.polygon = shape(self.lake_information['geometry'])
        self.logger.info("Extracted Polygon")
        feature.Destroy()
        hydro_lakes.ReleaseResultSet(result)

    def get_lake_polygon(self) -> Polygon:
        """Reads in the geojson and gets the lake polygon"""
        # Read it again as a shapely polygon (I think because it is simple from a GeoJSON)
        if self.lake_information is None:
            raise ValueError("Lake information not extracted, call extract_lake_information() first")

        return self.polygon

    def get_lake_name(self) -> str:
        """Extracts the lake name"""
        if self.lake_information is None:
            raise ValueError("Lake information not extracted, call extract_lake_information() first")

        return self.lake_name


def extract_watershed_of_lake(polygon: Polygon) -> Polygon:
    """
    Extract lake watershed polygon using HydroSheds dataset
    Parameters
    ----------
    polygon : Polygon
        Polygon of the lake
    Returns
    -------
    Polygon
        Polygon of the watershed
    """
    raise Exception("Not implemented yet")
    return None


def subset_box(da: xr.Dataset, poly: Polygon, pad=1) -> xr.Dataset:
    """FROM ONLINE SOURCE!!!!! Subset an xarray object to the smallest box including the polygon.
    Assuming the polygon is defined as lon/lat and that those are variables in da.
    A mask if first constructed for all grid centers falling within the bounds of the polygon,
    then a padding of {pad} cells is added around it to be sure all points are included.

    Parameters
    ----------
    da : xr.Dataset
        The dataset to subset
    poly : Polygon
        The polygon to use for subsetting
    pad : int
        The number of cells to add around the polygon

    Returns
    -------
    xr.Dataset
        The subsetted dataset
    """
    dims = da.lon.dims
    if len(dims) == 1:
        dims = ['lon', 'lat']

    if hasattr(poly, 'total_bounds'):
        min_lon, min_lat, max_lon, max_lat = poly.total_bounds
    else:
        min_lon, min_lat, max_lon, max_lat = poly.bounds

    mask = (da.lon >= min_lon) & (da.lon <= max_lon) & (da.lat >= min_lat) & (da.lat <= max_lat)

    if mask.sum() == 0:
        raise ValueError(('The returned mask is empty. Either the polygons do not overlap with '
                          'the grid or they are too small. In the latter case, try adding a '
                          'buffer: subset_box(da, poly.buffer(0.5)).'))

    for dim in dims:
        mask = mask.rolling(**{dim: 2 * pad + 1}, min_periods=1, center=True).max()
    logger.info("Subsetted the dataset {} to lake area".format(da.attrs["product_name"]))
    return da.where(mask.astype(int), drop=True)


if __name__ == "__main__":
    print("This is the lake extraction module")
    print("It is not meant to be run directly")
