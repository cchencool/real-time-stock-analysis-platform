#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pojo import datavo, modelvo

import time
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext, DStream

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pojo.datavo import DataVO
from utils.sparkresource import SparkResource
from utils.processenum import ProcessStatus, ProcessCommand
from utils.moduletools import reflect_inst
from processor.processors import StockAvgProcessor, BaseProcessor, ProcessorResult

from multiprocessing import Process, Pipe

counter = 0
Process.daemon = True


class OLAPService(object):

    def __init__(self):#, master='local[2]', stream_port=5003, *args, **kwargs):
        pass

    def add_service(self, processor_name:str):
        pass

    def terminate_process(self, pid):
        pass

    def send_cmd(self, pid, cmd):
        pass


class OLAPProcess(Process):
    """
    start a single processor for each task
    OLAPProcess should terminate in a reasonable timespan
    """
    def __init__(self, processor_name, pip_conn, master='local[2]' , stream_port=5003, lifespan=30, batchDuration=5):
        pass

    def run(self):
        pass

    def terminate(self):
        super().terminate()
