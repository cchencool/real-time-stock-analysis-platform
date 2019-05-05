#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Time    : 2019-04-14 19:58
# @Author  : chen
# @Site    : 
# @File    : regression.py
# @Software: PyCharm
from pyspark.sql import DataFrame

__author__ = "chen"

from processor.processors import BatchModelProcessor, ProcessorResult

__all__ = ["LinearRegression"]


class LinearRegression(BatchModelProcessor):

    def transform(self, data: DataFrame) -> DataFrame:
        pass

    def modeling(self, data: DataFrame) -> DataFrame:
        pass

    def inference(self, data: DataFrame) -> DataFrame:
        pass

    def encapsulate_inference_result(self, inference_result) -> ProcessorResult:
        pass