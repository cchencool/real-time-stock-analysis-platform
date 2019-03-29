#!/usr/bin/env python3
# -*- coding: utf-8 -*-

class DataAccess(object):

    def __init__(self):
        pass

    def run_sql(self, sql:str):
        """
        run arbitrary sql and get result
        :param sql: run sql
        :return: sql result
        """
        pass


    def get_data(self, table:str, is_batch:bool, *args) -> list:
        """
        query table data
        :param table: {org*|pred*|model*}
        :param is_batch: batch query or single query
        :param args: args for org data query
        :return:
        """
        pass


    def store_data(self, table:str, is_batch:bool, *args) -> bool:
        """
        store data into table
        :param table: {org*|pred*|model*}
        :param is_batch: store data in batch mode or single line
        :param args: data & params to store
        :return:
        """
        pass

