#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Time    : 2019-04-30 09:44
# @Author  : chen
# @Site    : 
# @File    : clusterregression.py
# @Software: PyCharm

from pyspark.streaming import DStream

__author__ = "chen"

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.mllib.regression import StreamingLinearRegressionWithSGD

from pyspark.sql.functions import monotonically_increasing_id, lag
from pyspark.sql.window import Window

import os
import sys
import random
import builtins
import numpy as np
import pandas as pd
from scipy import stats
from datetime import timedelta
from datetime import datetime as dt
from os.path import join as pjoin
from pyspark.sql.types import *
from itertools import islice
from datetime import datetime as dt
from pyspark.sql.functions import *
from pyspark.ml.feature import *
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from pyspark.streaming import StreamingContext

from processor.processors import StreamingProcessor, ProcessorResult
from pojo.datavo import DataVO

__all__ = ["ClusterRegressionProcessor"]


class ClusterRegressionProcessor(StreamingProcessor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cluster_num = int(kwargs.get('cluster_num', 2))

    def handle(self, data):
        # data.pprint()
        batchInterval = self.get_spark_stream_batchInterval()
        shift_count = 2 #batchInterval - 1 # leg shift batchInterval - 1 times.
        # using slide window to ensure the new coming data have enough predecessor to shift.
        # window width is twice as the batchInterval where each interval slide only 1 time of the batchInterval
        dataStream = data.window(batchInterval * 3, batchInterval)
        dataStream = dataStream.transform(lambda time, rdd: self.transformer(time, rdd))
        # dataStream.pprint()
        kmeanSchema = DataVO.get_schema()

        # cluster number
        numClusters = self.cluster_num
        # initial streaming clustering model
        # dataStream = dataStream
        dataStream = dataStream.transform(lambda time, rdd: MDLUtils.cluster_feature_builder(time, rdd, kmeanSchema, shift_count=shift_count))
        dataStream = dataStream.cache()
        # after transform shiftting features, drop the old useless data
        # dataStream = dataStream.window(batchInterval)

        ## split train/pred data stream for KMEANS, separate to 2 streams
        nonone_datastream = dataStream.filter(lambda v: None not in v)
        trainS = nonone_datastream.map(MDLUtils.cluster_parse_train)
        predS = nonone_datastream.map(MDLUtils.cluster_parse_pred).map(lambda lp: (lp.label, lp.features))
        # trainS.pprint()
        # predS.pprint()

        ## clustering
        # for now is features except (time, stock_code):  (len(kmeanSchema.names) - 2)
        # orgfeature_count = len(kmeanSchema.names) - 2
        orgfeature_count = 3 - 2
        # here dim may change due the different feature Engineering;
        #   then shift (batchInterval-1) times, combine with current value: *batchInterval
        #   and another one future price: + 1
        dim = (orgfeature_count) * (shift_count + 1) + 1
        model = StreamingKMeans(k=numClusters, decayFactor=1.0).setRandomCenters(dim, 1.0, 5003)  # dim weight seed
        # train & pred on streams get kmean_result: dstream ==> (label, clusterID)
        model.trainOn(trainS)
        kmean_result = model.predictOnValues(predS)
        # print(model.latestModel.centers)
        # the kmean_result will be like same label have multiple clusters, which each row belongs to 1 timepoint.
        # therefore, requring a groupby to eliminate the duplicated labels (stock_code).
        # using mode as cluster for the stock; require scipy.stats
        kmean_result = kmean_result.groupByKey().mapValues(lambda v: float(stats.mode(list(v)).mode[0]))
        # kmean_result.pprint()
        kmean_result.foreachRDD(lambda rdd: self.encapsulate_kmean(rdd))

        kmean_result = kmean_result.cache()

        # # kmean_result.foreachRDD(lambda rd: print(rd.lookup(1)))

        # this method must be here, not in another class as static method; is was used in map func.
        def flattenLine(v):
            r = []
            for l in v:
                if isinstance(l, list) or isinstance(l, tuple):
                    r.extend(flattenLine(l))
                else:
                    r.append(l)
            return r

        ## aggregation by cls & time
        # swap time & stock_code, then make stock_code as key -> [stock_code, time, ....]
        kstream = dataStream.map(lambda v: [v[1], v[0], v[1:]]).map(flattenLine).map(lambda v: (v[0], v[1:]))
        # kstream.pprint()
        # join the data points with label
        # (stock_code, (cluster_id, [time, stock_code, features:list, label]))
        jstream = kmean_result.join(kstream)
        # [cluster_id, time, stock_code, features:list, label], remove extra stock_code
        jstream = jstream.map(flattenLine).map(lambda v: v[1:])
        # jstream.pprint()
        schema_agg = MDLUtils.get_shifted_schema(orgfeature_count=orgfeature_count, shift_count=shift_count, orgschema=kmeanSchema)
        rdatastream = jstream.transform(lambda rdd: MDLUtils.regression_feature_builder(rdd, schema=schema_agg, st_idx=3, group_by=['cluster_id', 'time']))
        # [cluster_id, time, features, features_lag1, features_lag2, ..., label], remove extra stock_code
        rdatastream = rdatastream.cache()
        # rdatastream.pprint()

        # ([time], LabeledPoint(y, vec)) parser
        def rparse(lp, st_idx=2):
            y = -1 if lp[-1] is None else lp[-1] # change None label as negative clusterID for easier prediction
            vec = Vectors.dense(lp[st_idx:-1])
            k = lp[1] #[0:st_idx] #lp[1] if lp[1] is not None else -1
            return (k, LabeledPoint(y, vec))

        # must use a function to divide the branches of stream
        # to avoid using the the global varibale `cls` accidently
        def branch(gstream, cls):
            # 0: cluster_id; 1: time; 2:-1 -> features; -1: Y(label)
            return gstream.filter(lambda v: v[0] == cls).map(lambda v: rparse(v, 2))

        ## split data for clusters. stream branches
        # separate steamdata for different cluster
        cls_streams = dict()
        for cls in range(numClusters):
            #clsstream = branch(rdatastream, cls).cache()
            # clsstream = clsstream
            cls_streams[cls] = branch(rdatastream, cls)#.cache() #clsstream
            # cls_streams[cls].pprint()

        ## do regression
        # features exclude colume 'cluster_id', 'time', 'stock_code', 'lead1-Y'
        num_reg_features = len(schema_agg.names) - 4
        cls_train_stream = dict()
        cls_pred_stream = dict()
        cls_reg_models = dict()
        cls_reg_prediction = dict()
        for cls in range(numClusters):
            cls_train_stream[cls] = MDLUtils.rparse_train(cls_streams[cls])
            cls_pred_stream[cls] = MDLUtils.rparse_pred(cls_streams[cls])
            cls_train_stream[cls].pprint()
            cls_pred_stream[cls].pprint()

            '''
            FIXME: for Demo
            '''
            # reg model
            if cls not in cls_reg_models.keys():
                cls_reg_models[cls] = MDLUtils.make_reg_model(numFeatures=num_reg_features)
            cls_reg_models[cls].trainOn(cls_train_stream[cls])
            # cls_reg_models[cls].predictOn(cls_pred_stream[cls].map(lambda lp: lp.features))
            # print(f"intercept: {cls_reg_models[cls].latestModel.intercept}, weights: {cls_reg_models[cls].latestModel.weights}")

            cls_reg_prediction[cls] = cls_reg_models[cls].predictOnValues(cls_pred_stream[cls])
            # valstream
            cls_reg_prediction[cls].pprint()
            # cls_reg_prediction[cls] = cls_train_stream[cls]\
            #     .map(lambda v: (-1, builtins.round(v.label + random.randrange(-5, 5)*0.1*v.label, 2)))

            self.encapsulate_result(cls, real_stream=cls_streams[cls], pred_val_stream=cls_reg_prediction[cls])
        return

    # res_dic = {
    #     'cls01': {
    #         'time01': {'real': 1, 'pred': 1},
    #         'time02': {'real': 1, 'pred': 1},
    #         'time03': {'real': 1, 'pred': 1},
    #         'time04': {'real': 1, 'pred': 1},
    #          ...
    #     },
    #     'cls02': {
    #         'time01': {'real': 1, 'pred': 1},
    #         'time02': {'real': 1, 'pred': 1},
    #         'time03': {'real': 1, 'pred': 1},
    #         'time04': {'real': 1, 'pred': 1},
    #          ...
    #     },
    #     ...
    # }
    def encapsulate_result(self, cls, real_stream, pred_val_stream):
        real_stream.foreachRDD(lambda rdd: self.encapsulate_real(cls, rdd))
        # find time to predict -> (label=-1, time)
        pred_time_stream = real_stream.filter(lambda v: v[1].label == -1).map(lambda v: (v[1].label, dt.strftime(v[0], '%Y-%m-%d %H:%M:%S')))
        pred_stream = pred_time_stream.join(pred_val_stream)
        # (label=-1, (time, pred_val)) -> (time, pred_val)
        pred_stream = pred_stream.map(lambda v: v[1])
        # pred_stream.pprint()
        pred_stream.foreachRDD(lambda rdd: self.encapsulate_pred(cls, rdd))

    def encapsulate_real(self, cls, rdd):
        res_dic = self.run_result#.result_dict
        cls = str(cls)
        cls_dic = res_dic.get(cls, dict())
        # (time_dt, (label=real_price, features)) -> (time, real_price)
        data = rdd.map(lambda v: (dt.strftime(v[0], '%Y-%m-%d %H:%M:%S'), v[1].label)).collect()
        for time, val in data:
            data_dic = cls_dic.get(time, dict())
            data_dic.update({'real':val})
            cls_dic.update({time: data_dic})
        res_dic.update({cls: cls_dic})

    def encapsulate_pred(self, cls, rdd):
        res_dic = self.run_result#.result_dict
        cls = str(cls)
        cls_dic = res_dic.get(cls, dict())
        # (time, pred_price)
        data = rdd.collect()
        for time, val in data:
            if np.isnan(val): #np.nan:
                val = -0.5
            data_dic = cls_dic.get(time, dict())
            data_dic.update({'pred':val})
            cls_dic.update({time: data_dic})
        res_dic.update({cls: cls_dic})
        print(f"{res_dic}")

    def encapsulate_kmean(self, rdd):
        res_dic = self.run_result
        km_dic = dict() #res_dic.get('kmean', dict())
        data = rdd.collect()
        if len(data) > 0:
            for id, clz in data:
                clz = str(clz)
                km_dic.update({clz: km_dic.get(clz, 0) + 1})
        if len(km_dic) > 0:
            res_dic.update({'kmean': km_dic})
        print(km_dic)
    # result_dict = {
    #     'real': {
    #         'cls1': [[], [], [], [], ],
    #         'cls2': [[], [], [], [], ]
    #     },
    #     'pred': {
    #         'cls1': [[], [], [], [], ],
    #         'cls2': [[], [], [], [], ]
    #     }
    # }
    # def encapsulate_pred(self, cls, rdd):
    #     res_dic = self.run_result#.result_dict
    #     data_dic = res_dic.get('pred', dict())
    #
    #     # (label=-1, pred_price) -> pred_price
    #     data = rdd.map(lambda v: v[1]).collect()
    #     data_lst = data_dic.get(cls, [])
    #     data_lst.append(data)
    #
    #     data_dic.update({cls: data_lst})
    #     res_dic.update({'pred': data_dic})
    #
    #     print(f"pred: {data_dic}")


class MDLUtils(object):

    # @staticmethod
    # def cluster_feature_builder(time, rdd, schema, shift_count=2):
    #     # 1. aggregation
    #     # print((rdd.take(5)))
    #
    #     df = rdd.toDF(schema=schema)
    #     df = df.groupBy(col('time'), col('stock_code')) \
    #         .avg('price', 'change', 'volume', 'amount', 'type') \
    #         .select(col('time'), col('stock_code'),
    #                 bround('avg(price)', 2).alias('price'),
    #                 bround('avg(change)', 2).alias('change'),
    #                 bround('avg(volume)', 2).alias('volume'),
    #                 bround('avg(amount)', 2).alias('amount'),
    #                 bround('avg(type)', 2).alias('type')) \
    #         .orderBy(col('stock_code'), col('time'), ascending=[1, 1])
    #
    #     new_cols = []
    #     new_cols.extend(schema.names)
    #
    #     # try:
    #     #     # 2.1 mutiply features:bsn
    #     #     df = df.withColumn("bsn", df.volume * df.amount * df.type)
    #     #     # 2.2 MinMaxScaler
    #     #     for i in ['amount', 'volume', 'bsn']:
    #     #         vecAssembler = VectorAssembler(inputCols=[i], outputCol=i + "_new")
    #     #         mmScaler = MinMaxScaler(inputCol=i + "_new", outputCol='mm_' + i)
    #     #         pipeline = Pipeline(stages=[vecAssembler, mmScaler])
    #     #         pipeline_fit = pipeline.fit(df)
    #     #         df = pipeline_fit.transform(df)
    #     #
    #     #
    #     #     # 多了 amount_new, volume_new, bsn_new 和 mm_amount, mm_volume, mm_bsn 这些列
    #     #     # drop掉 amount_new, volume_new, bsn_new 和 amount, volume, bsn 这些列
    #     #     df = df.drop('amount_new', 'volume_new', 'bsn_new', 'amount', 'volume', 'bsn')
    #     #
    #     #     # 重命名 mm_ 列
    #     #     # df = df.withColumnRenamed("mm_amount", "amount")
    #     #     # df = df.withColumnRenamed("mm_volume", "volume")
    #     #     # df = df.withColumnRenamed("mm_bsn", "bsn")
    #     #     df = df.withColumn("amount", split(col("mm_amount", ",")).getItem(0))
    #     #     df = df.withColumn("volume", split(col("mm_volume", ",")).getItem(0))
    #     #     df = df.withColumn("bsn", split(col("mm_bsn", ",")).getItem(0))
    #     #
    #     #     df = df.drop('mm_amount', 'mm_volume', 'mm_bsn')
    #     #
    #     #     # 2.3 shift features
    #     #     new_cols.append('bsn')
    #     #
    #     # except Exception as e:
    #     #         print(e)
    #
    #     # 2. shift features
    #     Y_COL_NAME = 'lead1-Y'
    #     w = Window.partitionBy("stock_code").orderBy('time')  # .rangeBetween(3, 1)
    #     lead1_Y = lead(col('price'), count=1).over(w)
    #     df_fe = df.withColumn(Y_COL_NAME, lead1_Y)
    #     for i in range(shift_count):  # 2): # shiftting time span, better to be the batchInterval-1
    #         df_fe = MDLUtils.append_lags(df_fe, new_cols, w, i + 1)
    #     df_fe = df_fe.orderBy(col('stock_code'), col('time'), ascending=[1, 1])
    #
    #     # 3. move lable column Y (lead1-Y) to the last column
    #     cols = df_fe.columns
    #     cols.remove(Y_COL_NAME)
    #     cols.append(Y_COL_NAME)
    #     df_fe = df_fe.select(cols)
    #     # df_fe.show()
    #
    #     # 4. transback to rdd
    #     rdd_out = df_fe.rdd.map(MDLUtils.map_df_to_rdd)
    #
    #     # # 5. drop rows with None feature values after shift. Except for lead-Y, which is the last col of the rdd
    #     # rdd_out = rdd_out.filter(lambda v: None not in v[:-1])
    #
    #     # 6. drop time column, which is the first column.
    #     #     rdd_out = rdd_out.map(lambda v: v[1:])
    #
    #     return rdd_out

    @staticmethod
    def cluster_feature_builder(time, rdd, schema, shift_count=2):
        # 1. aggregation
        # print((rdd.take(5)))

        df = rdd.toDF(schema=schema)
        df = df.groupBy(col('time'), col('stock_code')) \
            .avg('price')\
            .select(col('time'), col('stock_code'),
                    bround('avg(price)', 2).alias('price'))\
            .orderBy(col('stock_code'), col('time'), ascending=[1, 1])

        new_cols = []
        new_cols.append('price')

        # 2. shift features
        Y_COL_NAME = 'lead1-Y'
        w = Window.partitionBy("stock_code").orderBy('time')  # .rangeBetween(3, 1)
        lead1_Y = lead(col('price'), count=1).over(w)
        df_fe = df.withColumn(Y_COL_NAME, lead1_Y)
        for i in range(shift_count):  # 2): # shiftting time span, better to be the batchInterval-1
            df_fe = MDLUtils.append_lags(df_fe, new_cols, w, i + 1)
        df_fe = df_fe.orderBy(col('stock_code'), col('time'), ascending=[1, 1])

        # 3. move lable column Y (lead1-Y) to the last column
        cols = df_fe.columns
        cols.remove(Y_COL_NAME)
        cols.append(Y_COL_NAME)
        df_fe = df_fe.select(cols)
        df_fe.show()

        # 4. transback to rdd
        rdd_out = df_fe.rdd.map(MDLUtils.map_df_to_rdd)

        # # 5. drop rows with None feature values after shift. Except for lead-Y, which is the last col of the rdd
        # rdd_out = rdd_out.filter(lambda v: None not in v[:-1])

        # 6. drop time column, which is the first column.
        #     rdd_out = rdd_out.map(lambda v: v[1:])

        return rdd_out

    @staticmethod
    def append_lags(df_fe, fields, w, i):
        except_set = set(['time', 'stock_code'])
        for c in fields:  # df.columns:
            if c in except_set:
                continue
            df_fe = df_fe.withColumn(f"{c}-lag{i}", lag(col(c), count=i).over(w))
        return df_fe

    # @staticmethod
    # def get_shifted_schema(orgfeature_count, shift_count, orgschema=DataVO.get_schema()):
    #     rfields = []
    #     rfield_types = []
    #     rfields.append('cluster_id')
    #     rfield_types.append(DoubleType())
    #     # ['time', 'stock_code',
    #     #  'price', 'change', 'volume', 'amount', 'type']
    #     # rfields.extend(orgschema.names)
    #     # [DoubleType(), TimestampType(), DoubleType(),
    #     #  DoubleType(), DoubleType(), DoubleType(), DoubleType(), DoubleType()]
    #     # rfield_types.extend(orgschema.fields)
    #     for field in orgschema.fields:
    #         rfields.append(field.name)
    #         rfield_types.append(field.dataType)
    #
    #     for i in range(shift_count):
    #         for j in range(orgfeature_count):
    #             # 'price-lag1', 'change-lag1', 'volume-lag1', 'amount-lag1', 'type-lag1',
    #             feature = orgschema.fields[j - orgfeature_count].name # iter: ... -5, -4, -3, -2, -1
    #             # DoubleType(), DoubleType(), DoubleType(), DoubleType(), DoubleType(),
    #             feature_type = orgschema.fields[j - orgfeature_count].dataType  # iter: ... -5, -4, -3, -2, -1
    #             rfields.append(f'{feature }-lag{i+1}')
    #             rfield_types.append(feature_type)
    #
    #     rfields.append('lead1-Y')
    #     rfield_types.append(DoubleType())
    #
    #     rFields = []
    #     for f, t in zip(rfields, rfield_types):
    #         rFields.append(StructField(f, t, True))
    #     schema = StructType(rFields)
    #     return schema

    @staticmethod
    def get_shifted_schema(orgfeature_count, shift_count, orgschema=DataVO.get_schema()):
        rfields = []
        rfield_types = []
        rfields.append('cluster_id')
        rfield_types.append(DoubleType())
        # ['time', 'stock_code',
        #  'price']
        # [DoubleType(), TimestampType(), DoubleType(),
        #  DoubleType()]
        for field in orgschema.fields:
            if field.name in ['time', 'stock_code', 'price']:
                rfields.append(field.name)
                rfield_types.append(field.dataType)

        for i in range(shift_count):
            # 'price-lag1'
            feature = 'price'
            # DoubleType()
            feature_type = DoubleType()
            rfields.append(f'{feature}-lag{i + 1}')
            rfield_types.append(feature_type)

        rfields.append('lead1-Y')
        rfield_types.append(DoubleType())

        rFields = []
        for f, t in zip(rfields, rfield_types):
            rFields.append(StructField(f, t, True))
        schema = StructType(rFields)
        return schema


    @staticmethod
    def make_reg_model(numFeatures):
        # inital streaming linear regression model
        mdl = StreamingLinearRegressionWithSGD(stepSize=0.001, numIterations=10);
        mdl.setInitialWeights(np.ones(numFeatures).tolist())
        return mdl

    @staticmethod
    def regression_feature_builder(rdd, schema, st_idx=3,
                                   group_by = ['cluster_id', 'time']):
        # aggregation & feature engineering for Regression model.
        # exclude first 3 cols: cluster_id, time, stock_code
        # ['price', 'change', 'volume', 'amount', 'type',
        #  'price-lag1', 'change-lag1', 'volume-lag1', 'amount-lag1', 'type-lag1',
        #  'price-lag2', 'change-lag2', 'volume-lag2', 'amount-lag2', 'type-lag2',
        #  'lead1-Y']
        avg_cols = schema.names[st_idx:]
        # [col('cluster_id'), col('time')]
        group_by = [col(c) for c in group_by]
        # order by col('cluster_id'), col('time')
        order_by = group_by
        ascending = np.ones(len(order_by)).astype('int').tolist()
        slt_cols = []
        # select col('cluster_id'), col('time')
        slt_cols.extend(group_by)
        # slt_cols.reverse() # reverse
        # current features ...
        # lag 1 features ...
        # lag 2 features ...
        # other lags ...
        # bround('avg(lead1-Y)', 2).alias('lead1-Y')
        slt_cols.extend([bround(f'avg({c})', 2).alias(f'{c}') for c in avg_cols])
        try:
            # print(rdd.take(5))
            df = rdd.toDF(schema=schema)
            df = df.groupBy(*group_by) \
                .avg(*avg_cols) \
                .select(*slt_cols) \
                .orderBy(*order_by, ascending=ascending)
            df.show()
            rdd_out = df.rdd.map(MDLUtils.map_df_to_rdd)
            return rdd_out
        except Exception as e:
            print(e)
            return rdd

    @staticmethod
    def map_df_to_rdd(v):
        d = v.asDict()
        r = []
        for k in d:
            # print(k)
            r.append(v[k])
        return r

    # @staticmethod
    # def flattenLine(v):
    #     r = []
    #     for l in v:
    #         if isinstance(l, list) or isinstance(l, tuple):
    #             r.extend(MDLUtils.flattenLine(l))
    #         else:
    #             r.append(l)
    #     return r

    # we make an input stream of vectors for training,
    # as well as a stream of vectors for testing
    @staticmethod
    def cluster_parse_pred(lp):
        feature_start_idx = 2
        # stock id; the 0th is time
        label = lp[1]  # float(lp[lp.find('(') + 1: lp.find(')')])
        # features
        vec = Vectors.dense(lp[feature_start_idx:])  # Vectors.dense(lp[lp.find('[') + 1: lp.find(']')].split(','))
        return LabeledPoint(label, vec)

    @staticmethod
    def cluster_parse_train(lp):
        feature_start_idx = 2
        # only features for clustering. here include the training Y but filterd None before call this function
        vec = Vectors.dense(
            lp[feature_start_idx:])  # Vectors.dense([float(x) for x in lp[lp.find('[') + 1: lp.find(']')].split(',')])
        return vec

    # split train/pred data stream for REGRESSION
    @staticmethod
    def rparse_pred(stream):
        # none label row is the row to predict
        # return stream.map(lambda v:v[1]).filter(lambda v: v.label == -1).map(lambda v: (v.label, v.features))
        return stream.map(lambda v:v[1]).filter(lambda v: v.label < 0).map(lambda v: (v.label, v.features))

    @staticmethod
    def rparse_train(stream):
        # rows with labels are the rows to train
        # return stream.map(lambda v:v[1]).filter(lambda v: v.label != -1)
        return stream.map(lambda v:v[1]).filter(lambda v: v.label > 0 and True not in np.isnan(v.features))

