#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from utils.sparkresource import SparkResource

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext, DStream

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pojo.datavo import DataVO
from processor.processors import StockAvgProcessor, BaseProcessor

import threading
from multiprocessing import Process, pool

class OLTPService(object):#SparkResource):
    """
    This service should in charge of:
    1. init spark-session / stream context
    2. manage the startup or shutdown of stream processor functions
    """
    def __init__(self, master='local[2]', stream_port=5003, *args, **kwargs):
        # super().__init__(*args, **kwargs)
        self.master = master
        self.stream_port = stream_port
        # self.config(master=master).build()
        # self.ss : SparkSession = super().get_spark_session()
        # self.sc : SparkContext = super().get_spark_context()

    def add_service(self, sps: BaseProcessor):
        # threading.Thread(target=self._run, args=(sps, )).start()
        p = Process(target=self._run, args=(sps,))
        p.start()
        # self._run(processor_name=processor_name)

    """
    should startup a single processor for each StreamingContext
    """
    def _run(self, sps): #processor_name):
        # sps = StockAvgProcessor(schema=DataVO.get_schema(), master=self.master, app_name=)
        sps.build()
        ssc = sps.get_spark_stream_context(batchDuration=5)
        # Create a DStream that will connect to hostname:port, like localhost:9999
        dstream = ssc.socketTextStream("localhost", self.stream_port)
        sps.handle_stream(dstream=dstream)
        # should run in parallel
        ssc.start()  # Start the computation
        ssc.awaitTermination()  # Wait for the computation to terminate

