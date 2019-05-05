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
        self._spark_conf : SparkConf = SparkConf()
        self._spark_session : SparkSession= None
        self._spark_context : SparkContext= None
        self._spark_stream_context : StreamingContext= None
        self._spark_stream_batchDuration = None

    def base_config(self, master='local[2]', app_name='sparkResource'):#, **kwargs):
        config = self._spark_conf
        config.setMaster(master)
        config.setAppName(app_name)
        return self

    def config(self, **kwargs):
        config = self._spark_conf
        spark_cores_max = kwargs.get("spark_cores_max", "4")
        spark_max_memory = kwargs.get("spark_max_memory", "4g")
        spark_executor_memory= kwargs.get("spark_executor_memory", "4g")
        spark_driver_memory = kwargs.get("spark_driver_memory", "2g")
        config.set('spark.max.memory', spark_max_memory)
        config.set('spark.cores.max', spark_cores_max)
        config.set('spark.executor.memory', spark_executor_memory)
        config.set('spark.driver.memory', spark_driver_memory)

        for k in kwargs:
            ck = k.replace('_', '.')
            config.set(ck, kwargs[k])

        return self

    def build_context(self):
        ss = SparkSession.builder.config(conf=self._spark_conf).getOrCreate()
        sc = ss.sparkContext
        self._spark_context = sc
        self._spark_session = ss
        return self

    def log_switch(self, on=False):
        if self._spark_context and on:
            self._spark_context.setLogLevel("INFO")
        elif self._spark_context and not on:
            self._spark_context.setLogLevel("OFF")
        return self

    def get_spark_conf(self):
        return self._spark_conf

    def get_spark_context(self) -> SparkContext:
        # ss = SparkSession.builder.config(conf=self._spark_conf).getOrCreate()
        # sc = ss.sparkContext
        # return sc
        return self._spark_context

    def get_spark_session(self) -> SparkSession:
        self._spark_session = SparkSession.builder.config(conf=self._spark_conf).getOrCreate()
        # return ss
        return self._spark_session

    def get_spark_stream_context(self, batchDuration=5) -> StreamingContext:
        self._spark_stream_batchDuration = batchDuration
        if not self._spark_stream_context:
            ssc = StreamingContext(self._spark_context, batchDuration=batchDuration)
            self._spark_stream_context = ssc
        # else:
        #     self.get_spark_session().
        return self._spark_stream_context

    def get_spark_stream_batchInterval(self):
        return self._spark_stream_batchDuration

