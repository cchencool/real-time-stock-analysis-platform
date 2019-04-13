#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Time    : 2019-04-13 13:48
# @Author  : chen
# @Site    : 
# @File    : moduletools.py
# @Software: PyCharm

__author__ = "chen"


def reflect_inst(clz_path, **kwargs):
    package_name = clz_path.rsplit('.', 1)[0]
    clz_name = clz_path.rsplit('.', 1)[1]
    clz_py = __import__(name=package_name, fromlist=[clz_name])
    clz = getattr(clz_py, clz_name)
    inst = clz(**kwargs)
    return inst

