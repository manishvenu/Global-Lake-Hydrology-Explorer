from flask import Flask, request, Response
from GLHE.CLAY import lake_extraction
import GLHE.CLAY.CLAY_driver as CLAY_driver
import GLHE.LIME.LIME_sample_dashboard
import json
import sys
import requests

app = Flask(__name__)


# api_acceptor = Api(app)
@app.route('/glhe', methods=['GET'])
def glhe():
    response = requests.get('http://localhost:4040/api/tunnels/')
    json_data = json.loads(response.text)
    url = json_data['tunnels'][1]['public_url']
    if 'curious' in url:
        url = json_data['tunnels'][0]['public_url']

    HYLAK_ID = request.args["hylak_id"]
    CLAY_driver_obj = CLAY_driver.CLAY_driver()
    output_folder_path = CLAY_driver_obj.main(HYLAK_ID)
    dict_response = {"URL": url}
    response = Response(json.dumps(dict_response))

    @response.call_on_close
    def process_after_request():
        GLHE.LIME.LIME_sample_dashboard.app.run()
        pass

    return response


# class GLHE_API(Resource):
#     # methods go here
#     def get(self):
#         HYLAK_ID = request.args["hylak_id"]
#         lake_extraction_object = lake_extraction.LakeExtraction()
#         lake_extraction_object.extract_lake_information(HYLAK_ID)
#         CLAY_driver_obj = CLAY_driver.CLAY_driver()
#         output_folder_path = CLAY_driver_obj.main(HYLAK_ID)
#         GLHE.LIME.LIME_sample_dashboard.app.run()
#
#         return {'forwarding_url': "http://127.0.0.1:8050/",
#                 'name': lake_extraction_object.get_lake_name()}, 200  # return data and 200 OK code
#
#     pass
#
#
# api_acceptor.add_resource(GLHE_API, '/glhe')  # and '/locations' is our entry point for Locations

if (__name__ == "__main__"):
    app.run()
