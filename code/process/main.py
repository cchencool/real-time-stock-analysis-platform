#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
from os.path import join as pjoin
from flask import Flask, request, make_response, jsonify

from stream_process_service import sps
from processor.processors import StockAvgProcessor
from pojo.datavo import DataVO

app = Flask(__name__)

with open('../../config/config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s][%(name)s][%(threadName)s][%(levelname)s][%(message)s]',
                    filename=pjoin(config['log_file_path'], 'log.log'))

logger = logging.getLogger(__name__)

SPARK_MASTER_URL = 'spark://localhost:7077'
counter = 0

'''
TODO: RESTful API Entry for data visualization layer
'''

@app.route("/")
def hello():
    return "Hello, world!"

@app.route("/add_stock_avg_processor")
def add_service():
    global counter
    counter += 1
    sap = StockAvgProcessor(schema=DataVO.get_schema(), master=SPARK_MASTER_URL, app_name=f'sap_{counter}')
    sps.add_oltp(sap)#processor_name="StockAvgProcessor")
    return 'success'

if __name__ == "__main__":
    sps.set_master(SPARK_MASTER_URL)
    app.run(debug=True)
    # sps.run()