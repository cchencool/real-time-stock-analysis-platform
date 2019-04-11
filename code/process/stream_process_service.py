#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import findspark
findspark.init()

import json
import logging

from os.path import join as pjoin
from datetime import datetime as dt

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pojo.datavo import DataVO

with open('../../config/config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s][%(name)s][%(threadName)s][%(levelname)s][%(message)s]',
                    filename=pjoin(config['log_file_path'], 'log.log'))

logger = logging.getLogger(__name__)

# fh = logging.FileHandler()
# fh.setLevel(logging.INFO)
# logger.addHandler(fh)


def encapsulate(line: list):
    date_time = line[0]
    price = line[1]
    change = line[2]
    volume = line[3]
    amount = line[4]
    typ = line[5]
    stock_code = line[6]
    return DataVO(stock_code=stock_code,
                 date_time=date_time,
                 price=price,
                 change=change,
                 volume=volume,
                 amount=amount,
                 typ=typ)


def create_schema():
    # create schema
    fields = ['time','price','change','volume','amount','type','stock_code']
    field_types = [TimestampType(), DoubleType(), DoubleType(), DoubleType(), DoubleType(), StringType(), StringType()]
    sfields = []
    for f, t in zip(fields, field_types):
        sfields.append(StructField(f, t, True))
    schema = StructType(sfields)
    return schema


def cast_types(line):
    try:
        l = [dt.strptime(line[0], '%Y-%m-%d %X'), float(line[1]), float(line[2]), float(line[3]), float(line[4]), line[5], line[6]]
    except ValueError as e:
        l = [dt(2000, 1, 1, 0, 0, 0), -1.0, -1.0, -1.0, -1.0, '-1', '-1']
    return l


def process(time, rdd):
    df = rdd.toDF(schema=schema)
    print(f"for time: {time}")
    print(f"{'-'*30}{time}{'-'*30}")
    df.createOrReplaceTempView("stock")
    # get the mean price of stock with volume larger than 150. TODO should do groupby
    ss.sql('select t.stock_code, mean(t.price) as mean_price '
           'from stock t '
           'where t.volume > 150 '
           'group by t.stock_code').show()
    # df.filter(df['volume'] > 150).select(mean(df['price'])).show()
    # df.show()


class StreamProcessService(object):

    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)


if __name__ == '__main__':
    # Create a local StreamingContext with two working thread and batch interval of 1 second
    # master = 'local[2]'
    master = 'spark://localhost:7077'
    # master = 'spark://123.207.86.218:7077'
    # sc = SparkContext("local[*]", "NetworkWordCount")
    config = SparkConf()
    config.setMaster(master)
    config.setAppName("StockAnalysis")
    config.set('spark.cores.max', "2")
    config.set('spark.executor.memory', '1g')
    # config.set('spark.driver.memory', '512m')
    # sc = SparkContext().getOrCreate(conf=config)
    sc = SparkContext(conf=config)
    sc.setLogLevel("OFF")

    ssc = StreamingContext(sc, 5)
    ss = SparkSession(sc)

    # Create a DStream that will connect to hostname:port, like localhost:9999
    dstream = ssc.socketTextStream("localhost", 5003)# Split each line into words

    # ['----'] -> ['-','-','-']
    lines = dstream.flatMap(lambda lines: lines.split('\n'))
    # lines.map(lambda line: line.split(',')).map(lambda rec: str(ecapsulate(rec))).pprint()
    # ['-', '-', '-'] -> [ ['f', 'f', 'f'], ['f', 'f', 'f'],... ]
    field_lines = lines.map(lambda line: line.split(','))
    # cast string to column schema
    field_lines_casted = field_lines.map(cast_types)
    # create schema
    schema = create_schema()
    # process each batch of rdds
    df = field_lines_casted.foreachRDD(process)

    # # Count each word in each batch
    # pairs = words.map(lambda line: line.split(','))
    # wordCounts = pairs.reduceByKey(lambda x, y: x + y)
    # # Print the first ten elements of each RDD generated in this DStream to the console
    #wordCounts.pprint()
    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
