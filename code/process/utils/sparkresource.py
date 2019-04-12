#!/usr/bin/env python
# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

__all__ = ["SparkResource"]


class SparkResource(object):

    def __init__(self):
        self.__spark_conf : SparkConf = None
        self.__spark_session : SparkSession= None
        self.__spark_context : SparkContext= None
        self.__spark_stream_context : StreamingContext= None

    def config(self, master='local[2]', app_name='StockAnalysis', **kwargs):
        config = SparkConf()
        config.setMaster(master)
        config.setAppName(app_name)

        spark_cores_max = kwargs.get("spark_cores_max", "2")
        spark_executor_memory= kwargs.get("spark_executor_memory", "1g")
        spark_driver_memory = kwargs.get("spark_driver_memory", "512m")
        config.set('spark.cores.max', spark_cores_max)
        config.set('spark.executor.memory', spark_executor_memory)
        config.set('spark.driver.memory', spark_driver_memory)

        self.__spark_conf = config
        return self

    def log_switch(self, on=False):
        if self.__spark_context and on:
            self.__spark_context.setLogLevel("INFO")
        elif self.__spark_context and not on:
            self.__spark_context.setLogLevel("OFF")
        return self

    def build(self):
        ss = SparkSession.builder.config(conf=self.__spark_conf).getOrCreate()
        sc = ss.sparkContext
        ssc = StreamingContext(sc, 5)
        self.__spark_context = sc
        self.__spark_session = ss
        self.__spark_stream_context = ssc
        return self

    # @property
    # def spark_conf(self):
    #     return self.__spark_conf
    #
    # @property
    # def spark_session(self):
    #     return self.__spark_session
    #
    # @property
    # def spark_context(self):
    #     return self.__spark_context
    #
    # @property
    # def spark_stream_context(self):
    #     return self.__spark_stream_context

    def get_spark_conf(self):
        return self.__spark_conf

    # @spark_context.getter
    def get_spark_context(self):
        return self.__spark_context

    def get_spark_session(self):
        return self.__spark_session

    def get_spark_stream_context(self, batchDuration=5):
        if not self.__spark_stream_context:
            ssc = StreamingContext(self.__spark_context, batchDuration=batchDuration)
            self.__spark_stream_context = ssc
        return self.__spark_stream_context
