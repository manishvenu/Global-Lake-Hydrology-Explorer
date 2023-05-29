from osgeo.gdal import OpenEx, OF_VECTOR
from shapely.geometry import shape, Polygon
from json import loads
import xarray as xr


def extract_lake(hylak_id: int) -> Polygon:
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
    hydro_lakes_shapefile_location = r'LocalData\HydroLAKES_polys_v10_shp\HydroLAKES_polys_v10.shp'
    hydro_lakes = OpenEx(hydro_lakes_shapefile_location, OF_VECTOR)
    layer = hydro_lakes.GetLayer()
    query = "SELECT * FROM {} WHERE hylak_id = \'{}\'".format(layer.GetName(), hylak_id)
    result = hydro_lakes.ExecuteSQL(query)
    feature = result.GetNextFeature()
    geojson_format = loads(feature.ExportToJson())
    polygon = shape(geojson_format['geometry'])
    feature.Destroy()
    hydro_lakes.ReleaseResultSet(result)
    return polygon


def subset_box(da: xr.Dataset, poly: Polygon, pad=1) -> xr.Dataset:
    """Subset an xarray object to the smallest box including the polygon.
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
    return da.where(mask.astype(int), drop=True)


if __name__ == "__main__":
    print("This is the lake extraction module")
    print("It is not meant to be run directly")
    s = extract_lake(1)
