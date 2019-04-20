#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Time    : 2019-04-14 20:03
# @Author  : chen
# @Site    : 
# @File    : clustering.py
# @Software: PyCharm

__author__ = "chen"

from processor.processors import StreamModelProcessor, ProcessorResult


class StreamKMeansProcessor(StreamModelProcessor):
    def modeling(self, df):
        pass

    def inference(self, df):
        pass

    def encapsulate_inference_result(self, inference_result) -> ProcessorResult:
        pass

