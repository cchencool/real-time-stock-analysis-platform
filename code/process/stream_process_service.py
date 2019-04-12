#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import findspark
findspark.init()

import json
import logging
import threading

import time
from os.path import join as pjoin
from datetime import datetime as dt

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext, DStream

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pojo.datavo import DataVO
from utils.sparkresource import SparkResource
from oltp_service import OLTPService
from olap_service import OLAPService

from processor.processors import StockAvgProcessor, BaseProcessor


# fh = logging.FileHandler()
# fh.setLevel(logging.INFO)
# logger.addHandler(fh)


class StreamProcessService():
    """
    should startup a single processor for each StreamingContext
    """
    def __init__(self, master='local[2]'):
        self.oltps = OLTPService(master=master)
        self.olaps = OLAPService(master=master)
        # self.dbp = DBStoreProcessor(schema=data_schema, master=master)
        # self.mbs = ModelBuildingService(master=master)

    def set_master(self, master):
        self.master = master
        self.oltps.master = master
        self.olaps.master = master

    def add_oltp(self, sps:BaseProcessor): #processor_name:str=""):
        self.oltps.add_service(sps) #processor_name)

    def add_olap(self, processor : BaseProcessor):
        self.olaps.add_service(sps=processor)

    @staticmethod
    def run(master='spark://localhost:7077'):
        pass
        # self.oltps.add_service(sap)
        # self.oltps.add_service(dbp)
        # self.oltps.add_service(mbs)

        # while True:
        #     # TODO should control the addition and deletion of
        #     time.sleep(10)


sps = StreamProcessService()

# if __name__ == '__main__':
#     StreamProcessService.run()
#     # OLTPService.run()

