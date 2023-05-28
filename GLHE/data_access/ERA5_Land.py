import cdsapi
import xarray as xr


def check_if_module_works() -> str:
    """A simple check if user is able to access functions in this file"""

    return "Success"

def get_ERA5_total() -> xr.Dataset:
    """Returns the Total ERA5 Local File, is very inefficient, also 17 GB and doesn't work on my computer

    Returns
    -------
    xarray Dataset
        xarray Dataset format of the evap, precip, & runoff in a grid of the total ERA5 area
    """

    return xr.load_dataset("GLHE/LocalData/ERA5LandTotal.grib",engine="cfgrib")

def get_total_precip_runoff_evap_in_subset_box_api(west: float, east: float, south: float, north: float) -> xr.Dataset:
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

    c = cdsapi.Client()
    try:
        c.retrieve(
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
            ".temp/TEMPORARY_DONOTOPEN_ERA5LAND.nc")
    except:
        print("Error calling API")
        return None
    return xr.open_dataset(".temp/TEMPORARY_DONOTOPEN_ERA5LAND.nc")
