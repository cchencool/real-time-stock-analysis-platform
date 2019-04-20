#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pojo import datavo, modelvo

import time
import logging
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext, DStream

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pojo.datavo import DataVO
from utils.sparkresource import SparkResource
from utils.processenum import ProcessStatus, ProcessCommand
from utils.moduletools import reflect_inst
from processor.processors import StockAvgProcessor, BaseProcessor, ProcessorResult, BatchModelProcessor
from database.dbaccess import DataAccess
from .manger import BaseTaskProcess, TaskManger

from multiprocessing import Process, Pipe

counter = 0
Process.daemon = True

class Condition(object):

    def __init__(self):
        pass


class SQLGen(object):

    def __init__(self, condition: Condition):
        self.condition = condition
        self.sql = None

    def genSQL(self):
        pass
        return self.sql


class OLAPProcess(BaseTaskProcess):
    """
    start a single processor for each task
    OLAPProcess should terminate in a reasonable timespan
    """
    def __init__(self, processor_name, pip_conn, **kwargs):
        super(OLAPProcess, self).__init__(processor_name=processor_name, pip_conn=pip_conn, **kwargs)
        self.condition = kwargs.get("condition")
        self.data_accessor = DataAccess()

    def run(self):
        """
        TODO
         1. database query to get dataframe
         2. passing dataframe to modelingProcessor.process
         3. get model &/or inference result
         4. invoke callback func
        :return:
        """
        # get SQL from self.condition
        # basic condition format {"daterange": "", "stock_code": "", "algo": ""}
        condition = Condition(**self.condition)
        sql = SQLGen(condition).genSQL()

        # get dataFrame from mongodb
        df = self.data_accessor.run_sql(sql=sql)

        self.status = ProcessStatus.RUNING
        # self.pip_conn.send(self.status)

        # handle data using processor
        self.sps:BatchModelProcessor
        self.sps.handle(df)

        # get result
        if self.sps.run_result is not None:
            self.status = ProcessStatus.FINISHED

        logging.info(f'processor {self.app_name} done')


