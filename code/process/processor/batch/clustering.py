#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Time    : 2019-04-15 11:05
# @Author  : chen
# @Site    : 
# @File    : clustering.py
# @Software: PyCharm
from pyspark.sql import DataFrame
from pyspark.streaming import DStream

__author__ = "chen"

from processor.processors import BatchModelProcessor, ProcessorResult

__all__ = ["KMeansProcessor"]


class KMeansProcessor(BatchModelProcessor):

    def transform(self, data: DataFrame) -> DataFrame:
        pass

    def modeling(self, data: DataFrame) -> DataFrame:
        pass

    def inference(self, data: DataFrame) -> DStream:
        pass

    def encapsulate_inference_result(self, inference_result) -> ProcessorResult:
        pass