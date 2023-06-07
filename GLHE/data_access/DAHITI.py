import json
import logging

import requests
from shapely.geometry import Polygon

from GLHE.helpers import MVSeries

logger = logging.getLogger(__name__)

def extract_lake_water_levels(lake_poly:Polygon) -> MVSeries:
    """Extracts lake water levels from DAHITI

    Parameters
    ----------
    lake_poly : Polygon
        The polygon to use for finding the lake

    Returns
    -------
    MVSeries
        The dataset of lake water levels
    """
    url = 'https://dahiti.dgfi.tum.de/api/v1/'
    args = {}
    args['username'] = 'manishvenu'
    args['password'] = 'NdFt8K2G@Yj3PX'
    # Find DAHITI ID
    args['action'] = 'get-nearest-target'
    args['longitude'] = lake_poly.centroid.x
    args['latitude'] =lake_poly.centroid.y
    response = requests.post(url, data=args)
    lake_dahiti_id = -1
    if response.status_code == 200:
        """ convert json string in python list """
        data = json.loads(response.text)
    else:
        print(response.status_code)
        logger.error("DAHITI ID not found for lake, no water levels to present, models cannot be run:(")
        return
    # Download data from DAHITI

    if data['data']['distance']>100:
        logger.error("Lake not found, no water levels to present, models cannot be run:(")
        return
    else:
        lake_dahiti_id = data['data']['id']
    rl = 'https://dahiti.dgfi.tum.de/api/v1/'

    args = {}
    """ required options """
    args['username'] = 'manishvenu'
    args['password'] = 'NdFt8K2G@Yj3PX'
    args['action'] = 'download-water-level'
    args['dahiti_id'] = lake_dahiti_id

    """ send request as method POST """
    response = requests.post(url, data=args)
    if response.status_code == 200:
        """ convert json string in python list """
        data = json.loads(response.text)
    else:
        print(response.status_code)


    ## Convert to MVSeries
