#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pojo import datavo, modelvo

from utils.sparkresource import SparkResource

class OLAPService(SparkResource):

    def __init__(self, master='local[2]', stream_port=5003, *args, **kwargs):
        return super().__init__(*args, **kwargs)