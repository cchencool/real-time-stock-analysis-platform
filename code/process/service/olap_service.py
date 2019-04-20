#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pojo import datavo, modelvo

import time
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext, DStream

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pojo.datavo import DataVO
from utils.sparkresource import SparkResource
from utils.processenum import ProcessStatus, ProcessCommand
from utils.moduletools import reflect_inst
from processor.processors import StockAvgProcessor, BaseProcessor, ProcessorResult
from .manger import BaseTaskProcess, TaskManger

from multiprocessing import Process, Pipe

counter = 0
Process.daemon = True


class OLAPProcess(BaseTaskProcess):
    """
    start a single processor for each task
    OLAPProcess should terminate in a reasonable timespan
    """
    def __init__(self, processor_name, pip_conn, **kwargs):
        super(OLAPProcess, self).__init__(processor_name=processor_name, pip_conn=pip_conn, **kwargs)
        self.condition = kwargs.get("condition")


    def run(self):
        """
        TODO
         1. database query to get dataframe
         2. passing dataframe to modelingProcessor.process
         3. get model &/or inference result
         4. invoke callback func
        :return:
        """
        pass
