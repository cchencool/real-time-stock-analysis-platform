#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from data_interface import DataInterface

class TushareImpl(DataInterface):

    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)

    def get_data(self):
        print('from tushare impl')

    def get_datasource(self):
        print('from tushare impl')