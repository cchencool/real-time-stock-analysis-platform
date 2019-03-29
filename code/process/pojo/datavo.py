#!/usr/bin/env python3
# -*- coding: utf-8 -*-

class DataVO(object):

    def __init__(self, **kargs):
        self.stock_code = kargs['stock_code']
        self.datetime = kargs['datetime']
        self.predict_val = kargs['predict_val']
        self.price_close = kargs['price_close']
        self.price_open = kargs['price_open']
        self.volume = kargs['volume']


