#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import findspark
findspark.init()

import json
import logging
import threading

import time
from os.path import join as pjoin
from datetime import datetime as dt

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext, DStream

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pojo.datavo import DataVO
from utils.sparkresource import SparkResource
from utils.processenum import ProcessCommand
from oltp_service import OLTPService
from olap_service import OLAPService

from processor.processors import StockAvgProcessor, BaseProcessor

__all__ = ['sps']


class StreamProcessService():
    """
    This layer should determine whether reuse the already started processor OR start a new processor to handle the task.
    """
    def __init__(self, master='local[2]'):
        self.processor_to_pid = dict()
        self.pid_to_processor = dict()
        self.oltps : OLTPService = OLTPService()#master=master)
        self.olaps_pid_dict = dict()
        self.olaps : OLAPService = OLAPService()#master=master)
        # self.dbp = DBStoreProcessor(schema=data_schema, master=master)
        # self.add_oltp('processor.processors.DBStoreProcessor')
        # self.mbs = ModelBuildingService(master=master)

    def set_master(self, master):
        self.master = master
        self.oltps.set_master(master)
        self.olaps.master = master

    def add_oltp(self, processor_name):
        # check processor/task cache to determine whether start a new one
        if processor_name in self.processor_to_pid:
            pid = self.processor_to_pid[processor_name]
        else:
            pid = self.oltps.add_service(processor_name=processor_name)
            self.processor_to_pid[processor_name] = pid
            self.pid_to_processor[pid] = processor_name
        return pid

    def add_olap(self, processor_name):
        if processor_name in self.processor_to_pid:
            pid = self.processor_to_pid[processor_name]
        else:
            pid = self.olaps.add_service(processor_name=processor_name)
            self.processor_to_pid[processor_name] = pid
            self.pid_to_processor[pid] = processor_name
        return pid

    def stop_oltp_processor(self, pid):
        pid = int(pid)
        result = self.oltps.terminate_process(pid)
        self.processor_to_pid.pop(self.pid_to_processor.pop(pid))
        return f"{result}"

    def get_oltp_curr_result(self, pid):
        pid = int(pid)
        result = self.oltps.communicate(pid=pid, cmd=ProcessCommand.GET_CURR_RESULT)
        return f"{result}"


# singleton
sps = StreamProcessService()
