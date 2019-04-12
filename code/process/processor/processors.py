#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Time    : 2019-04-12 14:19
# @Author  : chen
# @Site    : 
# @File    : processors.py
# @Software: PyCharm

__author__ = "chen"

import findspark
findspark.init()

import json
import logging
from datetime import datetime as dt

from pyspark.streaming import DStream

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pojo.datavo import DataVO
from utils.sparkresource import SparkResource


class BaseProcessor(SparkResource):
    """
    abstract class for processors
    """
    def __init__(self, schema : StructType, master='local[2]', app_name='baseProcessor', *args, **kwargs):
        # FIXME
        #  spark-session mng should not be here, but in OLTP or OLAP service.
        #  all processors should share the same spark-session & stream data.
        #  just using different process functions.
        super().__init__(*args, **kwargs)
        self.schema = schema
        self.ss = None
        self.sc = None
        self.master = master
        self.app_name = app_name

    def build(self, **kwargs):
        self.config(master=self.master, app_name=self.app_name)
        super().build()
        self.ss : SparkSession = super().get_spark_session()
        self.sc : SparkContext = super().get_spark_context()

    def handle_stream(self, dstream: DStream):
        """
        Stream Handler
        :param dstream: input dstream
        :return: None
        """
        dstream = self.format(dstream=dstream)
        # process rdds of each batch
        dstream.foreachRDD(self.processor)

    def format(self, dstream: DStream) -> DStream:
        """
        Transform the DStream to required structure.
        :param dstream: input dstream
        :return: transformed dstream
        """
        raise NotImplementedError

    def processor(self, time, rdd):
        """
        process program foreach rdd in DStream object
        :param time:
        :param rdd:
        :return:
        """
        raise NotImplementedError


class StockAvgProcessor(BaseProcessor):
    """
    sample process: get the mean price of stock with volume larger than 150.
    you can call model_building_service here
    """
    def format(self, dstream: DStream) -> DStream:
        """
        Transform the DStream to required structure.
        :param dstream: input dstream
        :return: transformed dstream
        """
        # ['----'] -> ['-','-','-']
        lines = dstream.flatMap(lambda lines: lines.split('\n'))
        # lines.map(lambda line: line.split(',')).map(lambda rec: str(DataVO.ecapsulate(rec))).pprint()
        # ['-', '-', '-'] -> [ ['f', 'f', 'f'], ['f', 'f', 'f'],... ]
        field_lines = lines.map(lambda line: line.split(','))
        # TODO re-implement cast_type function. do this based on self.schema object
        # cast string to column schema
        def cast_types(line: list) -> list:
            try:
                l = [dt.strptime(line[0], '%Y-%m-%d %X'), float(line[1]), float(line[2]), float(line[3]), float(line[4]), line[5], line[6]]
            except ValueError as e:
                l = [dt(2000, 1, 1, 0, 0, 0), -1.0, -1.0, -1.0, -1.0, '-1', '-1']
            return l
        field_lines_casted = field_lines.map(cast_types)
        return field_lines_casted

    def processor(self, time, rdd):
        df = rdd.toDF(schema=self.schema)
        print(f"{'-'*30}{time}{'-'*30}")
        # df.createOrReplaceTempView("stock")
        # self.ss.sql('select t.stock_code, mean(t.price) as mean_price '
        #             'from stock t '
        #             'where t.volume > 150 '
        #             'group by t.stock_code').show()
        result = df.filter('volume > 150')\
            .groupby('stock_code')\
            .mean('price')\
            .withColumnRenamed('avg(price)', 'mean_price')
        result.show()
