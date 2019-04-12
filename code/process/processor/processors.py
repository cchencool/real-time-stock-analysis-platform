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
    def __init__(self, schema : StructType, master='local[2]', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = schema
        self.config(master=master).build()
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
        # cast string to column schema
        field_lines_casted = field_lines.map(DataVO.cast_types)
        return field_lines_casted

    def processor(self, time, rdd):
        '''
        sample process: get the mean price of stock with volume larger than 150.
        you can call model_building_service here
        '''
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
