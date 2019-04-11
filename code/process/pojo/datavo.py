#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
