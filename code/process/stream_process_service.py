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

from processor.processors import StockAvgProcessor

with open('../../config/config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s][%(name)s][%(threadName)s][%(levelname)s][%(message)s]',
                    filename=pjoin(config['log_file_path'], 'log.log'))

logger = logging.getLogger(__name__)

# fh = logging.FileHandler()
# fh.setLevel(logging.INFO)
# logger.addHandler(fh)


class StreamProcessService():
    """
    should startup a single processor for each StreamingContext
    """
    @staticmethod
    def run(master = 'spark://localhost:7077'):
        oltps = OLTPService(master=master)
        threading.Thread(target=oltps.run, args=()).start()

        # olaps = OLAPService(master=master)
        # threading.Thread(target=olaps.run, args=()).start()

        while True:
            # TODO should control the addition and deletion of
            time.sleep(10)

        # # create schema
        # data_schema = DataVO.get_schema()
        # sps  = StockAvgProcessor(schema=data_schema, master=master)
        # ssc = sps.get_spark_stream_context(batchDuration=5)
        # # Create a DStream that will connect to hostname:port, like localhost:9999
        # dstream = ssc.socketTextStream("localhost", 5003)
        # sps.handle_stream(dstream=dstream)

        # # should run in parallel
        # # sps_db = DBStoreProcessor(schema=data_schema, master=master)

        # ssc.start()  # Start the computation
        # ssc.awaitTermination()  # Wait for the computation to terminate


if __name__ == '__main__':
    StreamProcessService.run()
    # OLTPService.run()

