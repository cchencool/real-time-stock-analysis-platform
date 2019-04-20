#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import logging
from os.path import join as pjoin
from flask import Flask, request

from utils.processenum import ProcessCommand
from service.manger import service_manger
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

# =============== debug start =================
@app.route("/mongodb")
def add_mongodb():
    pdemo = 'dbstore'
    pparam = aquire_pparam(pdemo)
    pid, data = service_manger.add_task(**pparam)
    return f'success, pid = {pid}'


@app.route("/demo")
def add_demo_task():
    pdemo = 'demo'
    pparam = aquire_pparam(pdemo)
    pid, data = service_manger.add_task(**pparam)
    return f'success, pid = {pid}'
# =============== debug end ====================


@app.route("/add_task")
def add_task():
    pname = request.values.get('pname')
    condition = request.values.get('condition', {})
    pparam = aquire_pparam(pname)
    pparam.update(condition)
    pid, data = service_manger.add_task(**pparam)
    # TODO maybe should be sync for OLAP
    #  for OLAP, return data
    return f"success, pid = {pid}, data={data}"


@app.route("/stop_oltp_processor")
def stop_processor():
    pid = request.values.get('pid')
    result = service_manger.terminate_process(pid=pid)
    return f"pid: {pid}, status: {result}"


@app.route("/get_curr_oltp_result")
def get_curr_oltp_result():
    pid = request.values.get('pid')
    result = service_manger.communicate(pid=pid, cmd=ProcessCommand.GET_CURR_RESULT)
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
    service_manger.set_master(spark_master_host)

    # TODO
    #  1. start db processor from sps (stream process service)
    #  2. start basic model building service from sps
    # sps.start_db_store()


def aquire_pparam(pname:str) -> dict:
    pname_dict = config['pname_dict']
    assert pname in pname_dict
    pparam = pname_dict[pname]
    assert isinstance(pparam, dict)
    return pparam


if __name__ == "__main__":
    init_server()
    app.run(host='localhost', port=5000, debug=True)
