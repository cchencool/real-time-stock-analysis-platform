#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import time
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext, DStream

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pojo.datavo import DataVO
from utils.sparkresource import SparkResource
from utils.processenum import ProcessStatus, ProcessCommand
from utils.moduletools import reflect_inst, castparam
from processor.processors import StockAvgProcessor, BaseProcessor, ProcessorResult

from multiprocessing import Process, Pipe

counter = 0
Process.daemon = True

__all__ = ['oltps']


class OLTPService(object):
    """
    This service should in charge of:
    1. manage the startup or shutdown of stream processor functions
    """
    def __init__(self, master='local[2]', stream_port=5003, *args, **kwargs):
        # super().__init__(*args, **kwargs)
        self.set_master(master=master)
        self.set_stream_port(port=stream_port)
        self.process_dict = dict()
        self.processor_to_pid = dict()
        self.pid_to_processor = dict()

    def set_master(self, master):
        self.master = master

    def set_stream_port(self, port=5003):
        self.stream_port = port

    def add_service(self, processor_name:str):
        global counter
        # TODO
        #  1. Add process management. process pool
        parent_conn, child_conn = Pipe()
        counter += 1
        # check processor/task cache to determine whether start a new one
        if processor_name in self.processor_to_pid:
            pid = self.processor_to_pid[processor_name]
        else:
            p = OLTPProcess(processor_name, child_conn, master=self.master, stream_port=self.stream_port)
            p.start()
            pid = p.pid
            self.process_dict[p] = (p, parent_conn)
            self.processor_to_pid[processor_name] = pid
            self.pid_to_processor[pid] = processor_name
        return pid
        # return p.pid

    @castparam({'pid':int})
    def terminate_process(self, pid):
        if pid in self.process_dict:
            p, conn = self.process_dict[pid]
            p:OLTPProcess
            p.terminate()
            return ""

    @castparam({'pid':int})
    def communicate(self, pid, cmd):
        if pid in self.process_dict:
            p, conn = self.process_dict[pid]
            conn.send(cmd)
            state, run_result = conn.recv()
            return state, run_result.result_str


class OLTPProcess(Process):
    """
    start a single processor for each StreamingContext
    and different from OLAP, OLTP process should be terminated by user or process manager. it will not terminate by itself
    """
    # def __init__(self, sps, pip_conn, stream_port=5003, lifespan=30, batchDuration=5):
    def __init__(self, processor_name, pip_conn, master='local[2]' , stream_port=5003, lifespan=30, batchDuration=5):
        self.master = master
        self.app_name = f'sap_{counter}'
        # using reflect to create process instance
        sps : BaseProcessor = reflect_inst(processor_name,
                                           schema=DataVO.get_schema(),
                                           master=self.master,
                                           app_name=self.app_name)
        self.sps = sps
        self.pip_conn = pip_conn
        self.lifespan = lifespan
        self.stream_port = stream_port
        self.batchDuration = batchDuration
        self.status = ProcessStatus.INIT
        super(OLTPProcess, self).__init__()

    def run(self):
        # setup spark-sessions
        self.sps.build()
        ssc = self.sps.get_spark_stream_context(batchDuration=self.batchDuration)

        # Create a DStream that will connect to hostname:port, like localhost:9999
        dstream = ssc.socketTextStream("localhost", self.stream_port)

        # handle stream data
        self.sps.handle_stream(dstream=dstream)

        # Start the computation
        ssc.start()
        self.status = ProcessStatus.RUNING
        # ssc.awaitTermination(self.lifespan) # timeout lifespan sec
        while True:
            cmd = self.pip_conn.recv()
            if cmd == ProcessCommand.TERMINATE:
                self.status = ProcessStatus.STOPPED
                self.ssc.stop(stopSparkContext=True) # False)
                # send Process end status & sps result
                self.pip_conn.send((self.status, self.sps.run_result))
                break
            elif cmd == ProcessCommand.GET_CURR_RESULT:
                self.pip_conn.send((self.status, self.sps.run_result))
            # elif finished normally:
            #     if self.status == ProcessStatus.RUNING:
            #         self.status = ProcessStatus.FINISHED


    def terminate(self):
        super().terminate()


# singleton
oltps = OLTPService()