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

# fh = logging.FileHandler()
# fh.setLevel(logging.INFO)
# logger.addHandler(fh)

'''
TODO: RESTful API Entry for data visualization layer
'''


@app.route("/")
def hello():
    return "Hello, world!"


# debug
@app.route("/add_stock_avg_processor")
def add_service():
    pid = sps.add_oltp('processor.processors.StockAvgProcessor')
    return f'success, pid = {pid}'


@app.route("/add_processor")
def add_oltp_service():
    pname = request.values.get('pname')
    pid = sps.add_oltp(pname)
    return f'success, pid = {pid}'


@app.route("/stop_oltp_processor")
def stop_processor():
    pid = request.values.get('pid')
    result = sps.stop_oltp_processor(pid)
    return f"pid: {pid}, status: {result}"


@app.route("/get_curr_oltp_result")
def get_curr_oltp_result():
    pid = request.values.get('pid')
    result = sps.get_oltp_curr_result(pid)
    return f"{result}"


if __name__ == "__main__":
    SPARK_MASTER_URL = 'spark://localhost:7077'
    sps.set_master(SPARK_MASTER_URL)
    app.run(host='localhost', port=5000, debug=True)
