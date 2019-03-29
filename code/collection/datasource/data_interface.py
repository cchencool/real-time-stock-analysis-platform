#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABCMeta,abstractmethod

class DataInterface(object):
    __metaclass__ = ABCMeta #指定这是一个抽象类
    @abstractmethod  #抽象方法
    def get_data(self):
        pass

    @abstractmethod
    def get_datasource(self):
        pass