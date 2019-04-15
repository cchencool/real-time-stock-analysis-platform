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

from pyspark import rdd as t_rdd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

from utils.sparkresource import SparkResource

__all__ = ['BaseProcessor', 'ProcessorResult',
           'ModelingProcessor', 'StreamModelProcessor', 'BatchModelProcessor',
           'StockAvgProcessor']


class ProcessorResult(object):

    def __init__(self):
        self.result_str = ''
        self.result_list = []
        self.result_dict = []
        self.inference_result = None


class BaseProcessor(SparkResource):
    """
    abstract class for processors.
    Some processor can implement as stateful computation
    Should be applied in single process, because the only one spark-session can running in one JVM
    """
    def __init__(self, schema:StructType, master='local[2]', app_name='baseProcessor'):
        super().__init__()
        self.schema = schema
        self.ss = None
        self.sc = None
        self.master = master
        self.app_name = app_name
        self.run_result = ProcessorResult()

    def build(self, **kwargs):
        """
        for extra spark config, re-implement this, should call self.config before self.build()
        :param kwargs:
        :return:
        """
        self.base_config(master=self.master, app_name=self.app_name)
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
        run_result = dstream.foreachRDD(self.processor)
        self.run_result = run_result
        # dstream.foreachRDD(self.done)

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

    def processor(self, time, rdd: t_rdd) -> ProcessorResult:
        """
        process program foreach rdd in DStream object.
        This part should also handle the run_result object if necessary.
        :param time:
        :param rdd:
        :return:
        """
        raise NotImplementedError


class ModelingProcessor(BaseProcessor):

    def __init__(self, schema:StructType, mdls=None, algo=None, master='local[2]', app_name='baseProcessor'):
        """
        Base class for modeling processors
        :param schema: data schema
        :param mdls: modeling service, mdls & algo should both be None or not.
        :param algo: model algorithm
        :param master: spark master host
        :param app_name: app name
        """
        super().__init__(self, schema=schema, master=master, app_name=app_name)
        self.algo = algo
        self.mdls = mdls
        self.model = None

    def processor(self, time, rdd:t_rdd=None, df:DataFrame=None) -> ProcessorResult:
        """
        oltp will pass rdd; olap will pass dataFrame by the framework
        :param time:
        :param rdd:
        :param df:
        :return:
        """
        if not df and rdd:
            df = rdd.toDF(schema=self.schema)
        # if self.mdls:
        #     self.model = self.mdls.modeling(df, algo=self.algo)
        #     result = self.mdls.inference(model=self.model, data=df, is_saving=False, saving_path=None)
        # else:
        self.model = self.modeling(df=df)
        result = self.inference(df=df)
        self.run_result = self.encapsulate_inference_result(result)

    def modeling(self, df):
        """
        if modeling service is not passed when init. using self-defined modeling func
        should include special feature engineering if needed
        :param df: the data is for both modeling and inference. (regression / clustering) for next n period
        :return:
        """
        raise NotImplementedError

    def inference(self, df):
        """
        if modeling service is not passed when init. using self-defined inference func
        :param df:
        :return:
        """
        raise NotImplementedError

    def encapsulate_inference_result(self, inference_result) -> ProcessorResult:
        """
        encapsulate inference result object returned by spark to system recognized format.
        :param inference_result:
        :return:
        """
        raise NotImplementedError


class StreamModelProcessor(ModelingProcessor):

    def processor(self, time, rdd:t_rdd) -> ProcessorResult:
        return super().processor(time=time, rdd=rdd)


class BatchModelProcessor(ModelingProcessor):

   def processor(self, time, df:DataFrame) -> ProcessorResult:
       return super().processor(time=time, df=df)


############################### classes above are all base class ####################################


# demo
class StockAvgProcessor(BaseProcessor):
    """
    sample process: get the mean price of stock with volume larger than 150.
    you can call model_building_service here
    """
    # def format(self, dstream: DStream) -> DStream:

    def build(self, **kwargs):
        self.config(spark_cores_max = '2');
        super().build()

    def processor(self, time, rdd:t_rdd):
        df = rdd.toDF(schema=self.schema)
        print(f"{'-'*30}{time}{'-'*30}")
        # df.createOrReplaceTempView("stock")
        # self.ss.sql('select t.stock_code, mean(t.price) as mean_price '
        #             'from stock t '
        #             'where t.volume > 150 '
        #             'group by t.stock_code').show()
        result = df.filter('volume > 150') \
            .groupby('stock_code') \
            .mean('price') \
            .withColumnRenamed('avg(price)', 'mean_price')
        result.show()
        self.run_result.result_str = time
        return self.run_result


