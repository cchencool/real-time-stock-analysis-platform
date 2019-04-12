#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime as dt
from pyspark.sql.types import *

class DataVO(object):

    def __init__(self, **kwargs):
        self.stock_code = kwargs['stock_code']
        self.date_time = kwargs['date_time']
        self.change = kwargs['change']
        self.price = kwargs['price']
        self.volume = kwargs['volume']
        self.amount = kwargs['amount']
        self.typ = kwargs['typ']

    def __str__(self):
        return str(f"stock record for stock: {self.stock_code}, at time: {self.date_time}")


    @staticmethod
    def create_schema() -> StructType:
        """
        Get DataFrame schema
        :return: StructType
        """
        fields = ['time', 'price', 'change', 'volume', 'amount', 'type', 'stock_code']
        field_types = [TimestampType(), DoubleType(), DoubleType(), DoubleType(), DoubleType(), StringType(),
                       StringType()]
        sfields = []
        for f, t in zip(fields, field_types):
            sfields.append(StructField(f, t, True))
        schema = StructType(sfields)
        return schema

    @staticmethod
    def cast_types(line: list) -> list:
        try:
            l = [dt.strptime(line[0], '%Y-%m-%d %X'), float(line[1]), float(line[2]), float(line[3]), float(line[4]), line[5], line[6]]
        except ValueError as e:
            l = [dt(2000, 1, 1, 0, 0, 0), -1.0, -1.0, -1.0, -1.0, '-1', '-1']
        return l


    @staticmethod
    def encapsulate_datavo(line: list):
        date_time = line[0]
        price = line[1]
        change = line[2]
        volume = line[3]
        amount = line[4]
        typ = line[5]
        stock_code = line[6]
        return DataVO(stock_code=stock_code,
                      date_time=date_time,
                      price=price,
                      change=change,
                      volume=volume,
                      amount=amount,
                      typ=typ)
