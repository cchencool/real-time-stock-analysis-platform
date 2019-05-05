#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import time
import logging
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext, DStream

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.streaming.listener import StreamingListener

from os.path import join as pjoin
from datetime import datetime as dt
from pojo.datavo import DataVO
from utils.sparkresource import SparkResource
from utils.processenum import ProcessStatus, ProcessCommand
from utils.moduletools import reflect_inst, castparam
from processor.processors import StockAvgProcessor, BaseProcessor, ProcessorResult
from .manger import BaseTaskProcess

from multiprocessing import Process, Pipe

counter = 0
Process.daemon = True


class OLTPProcess(BaseTaskProcess):
    """
    start a single processor for each StreamingContext
    and different from OLAP, OLTP process should be terminated by user or process manager. it will not terminate by itself
    """

    def listen_cmd(self):
        """
        Thread for listening command from main process.
        :return:
        """
        while True and self.status == ProcessStatus.RUNING:
            cmd = self.pip_conn.recv()
            if cmd == ProcessCommand.TERMINATE:
                self.status = ProcessStatus.STOPPED
                self.ssc.stop(stopSparkContext=True) # False)
                # send Process end status & sps result
                self.pip_conn.send((self.status, self.sps.run_result))
                break
            elif cmd == ProcessCommand.GET_CURR_RESULT:
                # import copy
                # incr_dic = copy.copy(self.sps.run_result)
                # self.sps.run_result.clear()
                self.pip_conn.send((self.status, self.sps.run_result))#incr_dic))
            # elif finished normally:
            #     if self.status == ProcessStatus.RUNING:
            #         self.status = ProcessStatus.FINISHED

    def run(self):
        try:
            self.sps.build_context()
            ssc = self.sps.get_spark_stream_context(batchDuration=self.batchDuration)

            # setup spark-sessions
            # Create a DStream that will connect to hostname:port, like localhost:9999
            dstream = ssc.socketTextStream("localhost", self.stream_port)

            # handle stream data
            self.sps.handle(data=dstream)

            # def batchComplete():
            #     import os
            #     from os.path import join as pjoin
            #     log_dir = os.environ['MSBD5003_PRJ_LOG_PATH']
            #     with open(pjoin(log_dir, 'signal'), 'r') as f:
            #         l = f.readline()
            #         if l == 'sendComplete':
            #             with open(pjoin(log_dir, 'signal'), 'w+') as f:
            #                 f.write('batchComplete')
            #
            # sl = StreamingListener()
            # sl.onBatchCompleted(batchComplete)
            # ssc.addStreamingListener(sl)
            # Start the computation
            ssc.start()
            self.status = ProcessStatus.RUNING
        except Exception as e:
            self.status = ProcessStatus.FAILED
            logging.error(e)
            print(e)

        # report start status
        self.pip_conn.send(self.status)

        # start listening threading for cmd
        import threading
        t_listen = threading.Thread(target=self.listen_cmd)
        t_listen.start()

        ssc.awaitTermination()
        # ssc.awaitTermination(self.lifespan) # timeout lifespan sec

