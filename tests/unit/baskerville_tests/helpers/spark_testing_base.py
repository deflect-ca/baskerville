# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import os
import traceback

# from pyspark.sql.types import StructType
# from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from sparktestingbase.sqltestcase import SQLTestCase


class SQLTestCaseLatestSpark(SQLTestCase):
    def getConf(self):
        from pyspark import SparkConf
        conf = SparkConf()
        conf.set(
            'spark.sql.session.timeZone', 'UTC'
        )
        # set shuffle partitions to a low number, e.g. <= cores * 2 to speed
        # things up, otherwise the tests will use the default 200 partitions
        # and it will take a lot more time to complete
        # conf.set('spark.sql.shuffle.partitions', str(os.cpu_count() * 2))
        conf.set('spark.sql.shuffle.partitions', 2)
        conf.set(
            'spark.driver.memory', '2g'
        )
        return conf

    def setUp(self):
        try:
            from pyspark.sql import SparkSession
            self.session = SparkSession.builder.config(
                conf=self.getConf()
            ).appName(
                self.__class__.__name__
            ).getOrCreate()
            self.sqlCtx = self.session._wrapped
        except Exception:
            traceback.print_exc()
            from pyspark.sql import SQLContext
            self.sqlCtx = SQLContext(self.sc)

    def assertDataFrameEqual(self, expected, result, tol=0):
        expected = expected.select(expected.columns).orderBy(expected.columns)
        result = result.select(expected.columns).orderBy(expected.columns)
        super(SQLTestCaseLatestSpark, self).assertDataFrameEqual(
            expected, result, tol
        )

    def schema_helper(self, df, expected_schema, fields):
        new_schema = []

        # issue with nullable field
        for item in expected_schema:
            if item.name in fields:
                item.nullable = False
            new_schema.append(item)

        new_schema = StructType(new_schema)
        df = self.session.createDataFrame(
            df.rdd, schema=new_schema
        )
        return df

    def fix_schema(self, df, expected_schema, fields=None):
        """
        Since column nullables cannot be easily changed after dataframe has
        been created, given a dataframe df, an expected_schema and the fields
        that need the nullable flag to be changed, return a dataframe with the
        schema nullables as in the expected_schema (only for the fields
        specified)
        :param pyspark.sql.DataFrame df: the dataframe that needs schema
        adjustments
        :param pyspark.Schema expected_schema: the schema to be followed
        :param list[str] fields: the fields that need adjustment of the
        nullable flag
        :return: the dataframe with the corrected nullable flags
        :rtype: pyspark.sql.DataFrame
        """
        new_schema = []
        current_schema = df.schema
        if not fields:
            fields = df.columns

        for item in current_schema:
            if item.name in fields:
                for expected_item in expected_schema:
                    if expected_item.name == item.name:
                        item.nullable = expected_item.nullable
            new_schema.append(item)

        new_schema = StructType(new_schema)
        df = self.session.createDataFrame(
            df.rdd, schema=new_schema
        )
        return df

    def create_spark_sub_df(self, records):
        from pyspark.sql import functions as F
        sub_df = self.session.createDataFrame(
            records
        )
        sub_df = sub_df.withColumn(
            '@timestamp', F.col('@timestamp').cast('timestamp')
        )
        return sub_df

    def assertColumnExprEqual(self, expected_col, result_col):
        return self.assertEqual(str(expected_col._jc), str(result_col._jc))


class FeatureSparkTestCase(SQLTestCaseLatestSpark):

    def setUp(self):
        super().setUp()

    def get_df_with_extra_cols(self, feature, data, extra_cols=None):
        sub_df = self.create_spark_sub_df(data)
        sub_df = self.run_pre_group_by_calculations(feature, sub_df)
        sub_df = self.group_by(feature, sub_df)
        if extra_cols and isinstance(extra_cols, dict):
            for k, v in extra_cols.items():
                sub_df = sub_df.withColumn(k, v)
        sub_df = self.run_post_group_by_calcs(feature, sub_df)

        return sub_df

    def get_sub_df_for_feature(self, feature, records):

        sub_df = self.create_spark_sub_df(records)
        sub_df = self.run_feature_calcs(feature, sub_df)
        sub_df = self.group_by(feature, sub_df)

        return sub_df

    def run_feature_calcs(self, feature, df):
        df = self.run_pre_group_by_calculations(feature, df)
        df = self.add_cache_defaults(feature, df)
        df = self.run_post_group_by_calcs(feature, df)

        return df

    def add_cache_defaults(self, feature, df):
        for k, v in feature.cache_columns_defaults.items():
            if k not in df.columns:
                df = df.withColumn(k, v)
        return df

    def group_by(self, feature, sub_df):
        # creation of the group by
        aggs = [v.alias(k) for k, v in feature.group_by_aggs.items()]
        sub_df = sub_df.groupby('client_ip')  # todo: ip, target
        if aggs:
            sub_df = sub_df.agg(*aggs)
        else:
            sub_df = sub_df.agg()  # todo: will fail
        return sub_df

    def run_pre_group_by_calculations(self, feature, sub_df):
        # renamings
        for k, v in feature.columns_renamed.items():
            sub_df = sub_df.withColumnRenamed(k, v)

        # calculations on logs
        for k, v in feature.pre_group_by_calcs.items():
            sub_df = sub_df.withColumn(k, v)
        # calculations on logs that cannot be expressed as column operations
        sub_df = feature.misc_compute(sub_df)

        return sub_df

    def run_post_group_by_calcs(self, feature, df):
        # calculations on the grouped dataframe
        for k, v in feature.post_group_by_calcs.items():
            df = df.withColumn(k, v)
        return df
