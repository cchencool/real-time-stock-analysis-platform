#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import logging
from os.path import join as pjoin
from flask import Flask, request, make_response, jsonify

from utils.processenum import ProcessCommand
from oltp_service import oltps
from processor.processors import StockAvgProcessor
from pojo.datavo import DataVO
import threading


app = Flask(__name__)
sem = threading.Semaphore()

config = dict()
LOG_PATH = os.environ.get('MSBD5003_PRJ_LOG_PATH', '.')

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s][%(name)s][%(threadName)s][%(levelname)s][%(message)s]',
                    filename=pjoin(LOG_PATH, 'log.log'))

logger = logging.getLogger(__name__)

# fh = logging.FileHandler()
# fh.setLevel(logging.INFO)
# logger.addHandler(fh)

'''
TODO: RESTful API Entry for data visualization layer
RequestHandler
'''


@app.route("/")
def hello():
    return "Hello, world!"


# debug
@app.route("/demo")
def add_service():
    pdemo = 'demo'
    pname_dict = config['pname_dict']
    if pdemo in pname_dict:
        pdemo = pname_dict[pdemo]
    pid = oltps.add_service(pdemo)
    return f'success, pid = {pid}'


@app.route("/add_processor")
def add_oltp_service():
    pname = request.values.get('pname')
    pname_dict = config['pname_dict']
    if pname in pname_dict:
        pname = pname_dict[pname]
    pid = oltps.add_service(processor_name=pname)
    return f'success, pid = {pid}'


@app.route("/stop_oltp_processor")
def stop_processor():
    # pid = int(request.values.get('pid'))
    pid = request.values.get('pid')
    result = oltps.terminate_process(pid=pid)
    return f"pid: {pid}, status: {result}"


@app.route("/get_curr_oltp_result")
def get_curr_oltp_result():
    # pid = int(request.values.get('pid'))
    pid = request.values.get('pid')
    result = oltps.communicate(pid=pid, cmd=ProcessCommand.GET_CURR_RESULT)
    return f"{result}"


@app.route("/reload_cfg")
def reload_cfg():
    sem.acquire()
    global config
    with open('../../config/config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    sem.release()

def init_server(spark_master_host ='spark://localhost:7077'):
    global config
    reload_cfg()
    spark_master_host = os.environ.get('SPARK_MASTER_HOST', spark_master_host)
    # sps.set_master(spark_master_host)
    oltps.set_master(spark_master_host)

    # TODO
    #  1. start db processor from sps (stream process service)
    #  2. start basic model building service from sps
    # sps.start_db_store()


if __name__ == "__main__":
    app.run(host='localhost', port=5000, debug=True)
    init_server()
