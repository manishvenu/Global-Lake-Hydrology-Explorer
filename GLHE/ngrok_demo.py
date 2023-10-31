from flask import Flask, request, Response, redirect
import GLHE.CLAY.CLAY_driver as CLAY_driver
import GLHE.LIME.LIME_sample_dashboard
import json
import sys
import requests
import logging

logger = logging.getLogger(__name__)
app = Flask(__name__)

CLAY_driver_obj = CLAY_driver.CLAY_driver()


@app.route('/glhe', methods=['GET'])
def glhe():
    logger.info("API Request Received")
    CLAY_driver_obj = CLAY_driver.CLAY_driver()
    response = requests.get('http://localhost:4040/api/tunnels/')
    json_data = json.loads(response.text)
    url = json_data['tunnels'][1]['public_url']
    if 'curious' in url:
        url = json_data['tunnels'][0]['public_url']

    HYLAK_ID = request.args["hylak_id"]
    CLAY_driver_obj.main(HYLAK_ID)
    GLHE.LIME.LIME_sample_dashboard.prep_work(CLAY_driver_obj)
    response = redirect(url)

    @response.call_on_close
    def process_after_request():
        GLHE.LIME.LIME_sample_dashboard.app.run()
        pass

    return response


if (__name__ == "__main__"):
    app.run()
