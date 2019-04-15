#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Time    : 2019-04-14 19:58
# @Author  : chen
# @Site    : 
# @File    : regression.py
# @Software: PyCharm

__author__ = "chen"

from processor.processors import BatchModelProcessor, ProcessorResult


class LinearRegression(BatchModelProcessor):

    def modeling(self, df):
        pass

    def inference(self, df):
        pass

    def encapsulate_inference_result(self, inference_result) -> ProcessorResult:
        pass