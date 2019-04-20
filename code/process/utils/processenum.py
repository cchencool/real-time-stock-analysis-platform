#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Time    : 2019-04-13 10:33
# @Author  : chen
# @Site    : 
# @File    : processenum.py
# @Software: PyCharm

__author__ = "chen"


from enum import Enum, unique

@unique
class ProcessStatus(Enum):
    INIT = 'init'
    FAILED = 'failed'
    RUNING = 'running'
    STOPPED = 'stopped'
    FINISHED = 'finished'


@unique
class ProcessCommand(Enum):
    TERMINATE = "terminate"
    GET_CURR_RESULT = "get_current_result"
