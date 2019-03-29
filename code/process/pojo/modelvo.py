#!/usr/bin/env python3
# -*- coding: utf-8 -*-


class ModelVO(object):

    def __init__(self, **kargs):
        self.id = kargs['id']
        self.type = kargs['type']
        self.mllib_model = kargs['mllib_model']
        self.filepath = kargs['filepath']
