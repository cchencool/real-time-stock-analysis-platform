#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datasource.tushare_impl import TushareImpl
from datasource.datafile_impl import DatafileImpl


'''
TODO: produce stream data (could use kafka)
'''

if __name__ == "__main__":
    # test
    ds_tushare = TushareImpl()
    print(ds_tushare.get_data())