#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Time    : 2019-04-20 13:13
# @Author  : chen
# @Site    : 
# @File    : service.py
# @Software: PyCharm

__author__ = "chen"

#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import logging

from pojo.datavo import DataVO
from utils.processenum import ProcessStatus
from utils.moduletools import reflect_inst, castparam
from processor.processors import BaseProcessor

from multiprocessing import Process, Pipe
from threading import Lock

counter = 0
counter_locker = Lock()
cache_locker = Lock()
Process.daemon = True

__all__ = ['service_manger', 'BaseTaskProcess']


class TaskManger(object):
    """
    base service class:
    """
    def __init__(self, master='local[2]', stream_port=5003, **kwargs):
        self.set_master(master=master)
        self.set_stream_port(port=stream_port)
        self.process_dict = dict()
        self.processor_to_pid = dict()
        self.pid_to_processor = dict()

    def set_master(self, master):
        self.master = master

    def set_stream_port(self, port=5003):
        self.stream_port = port

    def add_task(self, classname:str, type:str):
        global counter, counter_locker
        # TODO
        #  1. Add process management. process pool
        parent_conn, child_conn = Pipe()
        counter_locker.acquire()
        counter += 1
        counter_locker.release()
        pid = None
        data = None
        # check processor/task cache to determine whether start a new one
        if classname in self.processor_to_pid:
            cache_locker.acquire()
            pid = self.processor_to_pid[classname]
            p, conn = self.process_dict[pid]
            cache_locker.release()
            p:Process
            if p.is_alive():
                logging.info(f"find cache process for {classname}, pid: {pid}")
            else:
                self.terminate_process(pid)
                pid = None
        # no cache, or process is already dead.
        if pid is None:
            from .oltp_service import OLTPProcess
            from .olap_service import OLAPProcess
            is_sync = False
            if type.upper() == 'OLTP':
                clz = OLTPProcess
            elif type.upper() == 'OLAP':
                clz = OLAPProcess
                is_sync = True
            else:
                raise ValueError(f"processor type should be either OLAP or OLTP! {type} was given")

            p = clz(classname, child_conn, master=self.master, stream_port=self.stream_port)
            try:
                p.start()
                pstatus = parent_conn.recv()
                # check processor status.
                if pstatus == ProcessStatus.FAILED:
                    raise Exception('processor failed')
                pid = p.pid
            except Exception as e:
                logging.error(e)
                print(e)
                pid = None
            if pid is not None:
                cache_locker.acquire()
                self.process_dict[pid] = (p, parent_conn)
                self.processor_to_pid[classname] = pid
                self.pid_to_processor[pid] = classname
                cache_locker.release()
                logging.info(f"start new process for {classname}, pid: {pid}")

                if is_sync: # sync for OLAP
                    p.join()
                    data = p.sps.run_result
        return pid, data

    @castparam({'pid':int})
    def terminate_process(self, pid):
        result = "pid not found"
        cache_locker.acquire()
        if pid in self.process_dict:
            p, conn = self.process_dict.pop(pid)
            self.processor_to_pid.pop(self.pid_to_processor.pop(pid))
            cache_locker.release()
            try :
                p:BaseTaskProcess
                p.terminate()
            except Exception as e:
                pass
            result = "success"
        else:
            cache_locker.release()
        return result

    @castparam({'pid':int})
    def communicate(self, pid, cmd):
        status = None
        result = None
        cache_locker.acquire()
        if pid in self.process_dict:
            p, conn = self.process_dict[pid]
            cache_locker.release()
            conn.send(cmd)
            status, run_result = conn.recv()
            result = run_result.result_str
        else:
            cache_locker.release()
        return status, result


class BaseTaskProcess(Process):
    """
    start a single processor for each StreamingContext
    and different from OLAP, OLTP process should be terminated by user or process manager. it will not terminate by itself
    """
    def __init__(self, processor_name, pip_conn, **kwargs):

        super(BaseTaskProcess, self).__init__()
        self.pip_conn = pip_conn
        self.app_name = f'sap_{counter}'
        self.status = ProcessStatus.INIT
        self.master = kwargs.get('master', 'local[2]')
        self.lifespan = kwargs.get('lifespan', 30)
        self.stream_port = kwargs.get('stream_port', 5003)
        self.batchDuration = kwargs.get('batchDuration', 5)

        # using reflect to create process instance
        self.sps : BaseProcessor = reflect_inst(processor_name,
                                                schema=DataVO.get_schema(),
                                                master=self.master,
                                                app_name=self.app_name)

    def run(self):
        raise NotImplementedError

    def terminate(self):
        try:
            super().terminate()
        except Exception as e:
            pass


service_manger = TaskManger()