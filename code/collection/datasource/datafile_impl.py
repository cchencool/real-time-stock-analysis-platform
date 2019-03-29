#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from data_interface import DataInterface

class DatafileImpl(DataInterface):

    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)

    def get_data(self):
        print('from datafile impl')

    def get_datasource(self):
        print('from datafile impl')