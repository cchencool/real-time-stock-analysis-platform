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
from utils.processenum import ProcessStatus
from utils.sparkresource import SparkResource
from database.dbaccess import DataAccess

__all__ = ['BaseProcessor', 'StockAvgProcessor', 'DBStoreProcessor']


class ProcessorResult(object):

    def __init__(self):
        self.result_str = ''
        self.result_list = []
        self.result_dict = []


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
        dstream.foreachRDD(self.processor)
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

    def processor(self, time, rdd):
        """
        process program foreach rdd in DStream object.
        This part should also handle the run_result object if necessary.
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
    # def format(self, dstream: DStream) -> DStream:

    def build(self, **kwargs):
        self.config(spark_cores_max = '2');
        super().build()

    def processor(self, time, rdd):
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

    # def done(self, time, rdd):
    #     pass


'''
Author: Dojo
'''
class DBStoreProcessor(BaseProcessor):
    """
    Connect to the MongoDB
    """
    # def format(self, dstream: DStream) -> DStream:

    def build(self, **kwargs):
        self.config(spark_mongodb_input_uri="mongodb://127.0.0.1/5003.stocks",
                    spark_mongodb_output_uri="mongodb://127.0.0.1/5003.stocks",
                    spark_jars_packages="org.mongodb.spark:mongo-spark-connector_2.11:2.4.0")
        super().build()
        # self.ss.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
        # self.ss.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")

    def processor(self, time, rdd):
        # df = rdd.toDF(schema=self.schema)
        # df = self.ss.createDataFrame(rdd, self.schema)
        # df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

        # Store data
        # dao = DataAccess(self.ss)
        # dao.store_data(rdd, self.schema)

        db = '5003'
        coll = 'stocks'
        # Read data
        dao = DataAccess(self.ss)
        df = dao.get_data(db, coll, True)
        print(df)

        # Run SQL

        sql = 'select * from {} where type > 0'.format(coll)
        sparkDF_sql = dao.run_sql(sql, db, coll)
        df_sql = sparkDF_sql.collect()
        print(df_sql)

        # people = self.ss.createDataFrame([("Dojo", 20), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
        #      ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)], ["name", "age"])
        #
        # people.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

        # df = self.ss.read.format("com.mongodb.spark.sql.DefaultSource").load()

        # df = self.ss.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://127.0.0.1/test.myCollection").load()
        #
        # df.show()

# if __name__ == '__main__':
#     dbsp = DBStoreProcessor(schema=None, master='spark://zhengdongjiadeMacBook-Pro.local:7077', app_name='test_db_store')
#     dbsp.build()
#     dbsp.processor(time='', rdd='')