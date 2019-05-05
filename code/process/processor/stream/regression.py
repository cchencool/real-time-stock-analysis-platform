#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Time    : 2019-04-14 19:56
# @Author  : chen
# @Site    : 
# @File    : regression.py
# @Software: PyCharm
from pyspark.streaming import DStream

__author__ = "chen"

from processor.processors import StreamModelProcessor, ProcessorResult

__all__ = ["StreamLinearRegressionProcessor"]


class StreamLinearRegressionProcessor(StreamModelProcessor):

    def transform(self, data: DStream) -> DStream:
        pass

    def modeling(self, data: DStream) -> DStream:
        pass

    def inference(self, data: DStream) -> DStream:
        pass

    def encapsulate_inference_result(self, inference_result) -> ProcessorResult:
        pass