#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Time    : 2019-04-15 11:05
# @Author  : chen
# @Site    : 
# @File    : clustering.py
# @Software: PyCharm

__author__ = "chen"

from processor.processors import BatchModelProcessor, ProcessorResult


class KMeansProcessor(BatchModelProcessor):

    def modeling(self, df):
        pass

    def inference(self, df):
        pass

    def encapsulate_inference_result(self, inference_result) -> ProcessorResult:
        pass