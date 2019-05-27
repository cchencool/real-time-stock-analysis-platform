#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark import SparkConf

class DataAccess(object):

    def __init__(self, sps: SparkSession):
        self.sparkSession = sps

    def run_sql(self, sql:str, db: str, coll: str):
        """
        run arbitrary sql and get result
        :param sql: run sql (table = coll)
        :param db: database
        :param coll: collection or table (coll = table)
        :return: sql result
        """
        uri = "mongodb://127.0.0.1/{}.{}".format(db, coll)
        try:
            df = self.sparkSession.read.transform("com.mongodb.spark.sql.DefaultSource"). \
                option("uri", uri). \
                load()
            df.createOrReplaceTempView(coll)    # Notice that the table name is equal to 'coll'
            # a_employees = self.sparkSession.sql("SELECT * FROM temp WHERE FirstName LIKE '%a%'")
        except:
            print("Run SQL process failed!")
            return None
        else:
            result = self.sparkSession.sql(sql)
            print("Run SQL process succeeded!")
            return result

    def get_data(self, db:str, coll:str, is_batch:bool, *args) -> list:
        """
        query table data
        :param table: {org*|pred*|model*}
        :param is_batch: batch query or single query
        :param args: args for org data query
        :return:
        """
        uri = "mongodb://127.0.0.1/{}.{}".format(db, coll)
        try:
            df = self.sparkSession.read.transform("com.mongodb.spark.sql.DefaultSource"). \
            option("uri", uri). \
            load()
            result = df.collect()
        except:
            print("Get data process failed!")
            return  None
        else:
            print("Get data process succeeded!")
            return result


    def store_data(self, rdd, schema, *args) -> bool:
        """
        store data into table
        :param rdd: the source data in rdd
        :param schema: the schema of the data
        :param args: data & params to store
        :return:
        """
        try:
            df = self.sparkSession.createDataFrame(rdd, schema)
            df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
        except Exception as e:
            print("Store data process failed!")
            print(e)
            return False
        else:
            print("Store data process succeeded!")
            return True

