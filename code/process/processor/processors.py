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
from database.dbaccess import DataAccess

__all__ = ['BaseProcessor', 'ProcessorResult',
           'ModelingProcessor', 'StreamModelProcessor', 'BatchModelProcessor',
           'StockAvgProcessor', 'ModelInferenceProcessor', 'DBStoreProcessor']


class ProcessorResult(object):

    def __init__(self):
        self.result_str = ''
        self.result_list = []
        self.result_dict = {}
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
        self.socket_host = None
        self.socket_port = None
        self.ssc_batchDuration = None

    def build_context(self, **kwargs):
        """
        for extra spark config, re-implement this, should call self.config before self.build_context()
        :param kwargs:
        :return:
        """
        self.base_config(master=self.master, app_name=self.app_name)
        super().build_context()
        self.ss : SparkSession = super().get_spark_session()
        self.sc : SparkContext = super().get_spark_context()

    def transform(self, data):
        """
        transform before handle
        :param data:
        :return:
        """
        raise NotImplementedError

    def handle(self, data):
        """
        Stream Handler
        :param data: input dstream
        :return: None
        """
        raise NotImplementedError


class StreamingProcessor(BaseProcessor):

    def transform(self, dstream: DStream) -> DStream:
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

    def handle(self, data: DStream):
        """
        Stream Handler
        :param data: input dstream
        :return: None
        """
        try:
            data = self.transform(dstream=data)
            # process rdds of each batch
            data.foreachRDD(self.processor)
        except Exception as e:
            logging.error(e)
            print(e)
            return

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

    def __init__(self, schema:StructType, algo=None, master='local[2]', app_name='baseProcessor'):
        """
        Base class for modeling processors
        :param schema: data schema
        :param algo: model algorithm
        :param master: spark master host
        :param app_name: app name
        """
        super().__init__(self, schema=schema, master=master, app_name=app_name)
        self.algo = algo
        self.model = None

    def handle(self, data):
        data = self.transform(data=data)
        self.model = self.modeling(data=data)
        result = self.inference(data=data)
        self.run_result = self.encapsulate_inference_result(result)

    def modeling(self, data):
        """
        if modeling service is not passed when init. using self-defined modeling func
        should include special feature engineering if needed
        :param dstream: the data is for both modeling and inference. (regression / clustering) for next n period
        :return:
        """
        raise NotImplementedError

    def inference(self, data):
        """
        if modeling service is not passed when init. using self-defined inference func
        :param dstream:
        :return:
        """
        raise NotImplementedError

    def encapsulate_inference_result(self, inference_result) -> ProcessorResult:
        """
        encapsulate inference result object returned by spark to system recognized transform.
        :param inference_result:
        :return:
        """
        raise NotImplementedError


class StreamModelProcessor(ModelingProcessor):

    def transform(self, data: DStream) -> DStream:
        raise NotImplementedError

    def modeling(self, data: DStream) -> DStream:
        raise NotImplementedError

    def inference(self, data: DStream) -> DStream:
        raise NotImplementedError


class BatchModelProcessor(ModelingProcessor):

    def transform(self, data: DataFrame) -> DataFrame:
        raise NotImplementedError

    def modeling(self, data: DataFrame) -> DataFrame:
        raise NotImplementedError

    def inference(self, data: DataFrame) -> DStream:
        raise NotImplementedError


############################### classes above are all base class ####################################


# demo
class StockAvgProcessor(StreamingProcessor):
    """
    sample process: get the mean price of stock with volume larger than 150.
    you can call model_building_service here
    """
    # def transform(self, dstream: DStream) -> DStream:
    def build_context(self, **kwargs):
        self.config(spark_cores_max = '2');
        super().build_context()

    def processor(self, time, rdd:t_rdd):
        try :
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
        except Exception as e:
            # TODO
            #   if error found. stop processor
            #   for some OLTP, may need to restart the processor.
            logging.error(e)
            print(e)
            self.get_spark_stream_context().stop()


'''
Author: cchen
'''
class ModelInferenceProcessor(StreamModelProcessor):
    """
    TODO using batch model to do inference on the stream data
    """
    pass


'''
Author: Dojo
'''
class DBStoreProcessor(StreamingProcessor):
    """
    Connect to the MongoDB
    """
    # def transform(self, dstream: DStream) -> DStream:
    def build_context(self, **kwargs):
        self.config(spark_mongodb_input_uri="mongodb://127.0.0.1/5003.stocks",
                    spark_mongodb_output_uri="mongodb://127.0.0.1/5003.stocks",
                    spark_jars_packages="org.mongodb.spark:mongo-spark-connector_2.11:2.4.0")
        super().build_context()

    def processor(self, time, rdd):
        # Store data
        dao = DataAccess(self.ss)
        dao.store_data(rdd, self.schema)

        # Test the 'Get Data' & 'Run SQL' data access
        # db = '5003'
        # coll = 'stocks'
        # # Read data
        # dao = DataAccess(self.ss)
        # df = dao.get_data(db, coll, True)
        # print(df)
        #
        # # Run SQL
        # sql = 'select * from {} where type > 0'.transform(coll)
        # sparkDF_sql = dao.run_sql(sql, db, coll)
        # df_sql = sparkDF_sql.collect()
        # print(df_sql)


# if __name__ == '__main__':
#     dbsp = DBStoreProcessor(schema=None, master='spark://zhengdongjiadeMacBook-Pro.local:7077', app_name='test_db_store')
#     dbsp.build()
#     dbsp.processor(time='', rdd='')
