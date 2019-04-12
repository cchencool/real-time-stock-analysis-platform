#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import findspark
findspark.init()

import json
import logging

from os.path import join as pjoin
from datetime import datetime as dt

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext, DStream

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pojo.datavo import DataVO
from utils.sparkresource import SparkResource

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
    def run():
        # create schema
        data_schema = DataVO.create_schema()
        sps  = StockAvgProcessor(schema=data_schema, master='spark://localhost:7077')
        ssc = sps.get_spark_stream_context(batchDuration=5)
        # Create a DStream that will connect to hostname:port, like localhost:9999
        dstream = ssc.socketTextStream("localhost", 5003)
        sps.handle_stream(dstream=dstream)
        ssc.start()  # Start the computation
        ssc.awaitTermination()  # Wait for the computation to terminate


if __name__ == '__main__':
    StreamProcessService.run()

