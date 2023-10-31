import requests
import json
import pprint

url = 'https://dahiti.dgfi.tum.de/api/v1/'
id = 41478
args = {}
""" required options """
args['username'] = 'manishvenu'
args['password'] = 'NdFt8K2G@Yj3PX'
args['action'] = 'list-targets'
args['min_lon'] = -121
args['max_lon'] = -118
args['min_lat'] = 37
args['max_lat'] = 39

""" send request as method POST """
response = requests.post(url, data=args)

if response.status_code == 200:
    """ convert json string in python list """
    data = json.loads(response.text)
    closest = 100000000
    for i in data:
        print(i)
        if (abs(i['longitude'] - -119.0211) + abs(i['latitude'] - 38.0039)) < closest:
            closest = abs(i['longitude'] - -119.5) + abs(i['latitude'] - 37.5)
            id = i['id']
            print(id)
    print("Ans:", id)
else:
    print(response.status_code)

# args = {}
# """ required options """
# args['username'] = 'manishvenu'
# args['password'] = 'NdFt8K2G@Yj3PX'
# args['action'] = 'download-water-level'
# args['dahiti_id'] = str(id)
#
# """ send request as method POST """
# response = requests.post(url, data=args)
#
# if response.status_code == 200:
#     """ convert json string in python list """
#     data = json.loads(response.text)
#     pprint.pprint(data)
# else:
#     print(response.status_code)
