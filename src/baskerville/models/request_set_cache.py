# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import datetime
import gc
import os

from py4j.protocol import Py4JJavaError

from baskerville.spark import get_spark_session
from baskerville.spark.helpers import StorageLevel
from pyspark.sql import functions as F

from baskerville.util.file_manager import FileManager
from baskerville.util.helpers import get_logger, Singleton


class RequestSetSparkCache(Singleton):

    def __init__(
            self,
            cache_config,
            table_name,
            columns_to_keep,
            expire_if_longer_than=3600,
            logger=None,
            session_getter=get_spark_session,
            group_by_fields=('target', 'ip'),
            format_='parquet'
    ):
        self.__cache = None
        self.__persistent_cache = None
        self.schema = None
        self.cache_config = cache_config
        self.table_name = table_name
        self.columns_to_keep = columns_to_keep
        self.expire_if_longer_than = expire_if_longer_than
        self.logger = logger if logger else get_logger(self.__class__.__name__)
        self.session_getter = session_getter
        self.group_by_fields = group_by_fields
        self.format_ = format_
        self.storage_level = StorageLevel.CUSTOM
        self.column_renamings = {
            'first_ever_request': 'start',
            'old_subset_count': 'subset_count',
            'old_features': 'features',
            'old_num_requests': 'num_requests',
        }
        self._count = 0
        self._last_updated = datetime.datetime.utcnow()
        self._changed = False

    def load_empty(self, schema):
        """
        Instantiate an empty cache from the specified schema
        :param schema:
        :return:
        """
        self.schema = schema
        spark = self.session_getter()
        self.__cache = spark.createDataFrame([], schema)
        self.__persistent_cache = spark.createDataFrame([], schema)

    def update_df(
            self, df_to_update, join_cols=('target', 'ip'), select_cols=('*',)
    ):
        self._changed = True

        if "*" in select_cols:
            select_cols = self.cache.columns

        # add null columns if nothing in cache
        if len(self.__cache.head(1)) == 0:
            for c in select_cols:
                if c not in df_to_update.columns:
                    df_to_update = df_to_update.withColumn(c, F.lit(None))
            self.logger.warning('Cache is empty')
            return df_to_update

        # https://issues.apache.org/jira/browse/SPARK-10925
        df = df_to_update.rdd.toDF(df_to_update.schema).alias('a').join(
            self.cache.select(*select_cols).alias('cache'),
            list(join_cols),
            how='left_outer'
        )#.persist(self.storage_level)

        # update nulls and filter drop duplicate columns
        for c in select_cols:
            # if we have duplicate columns, take the new column as truth
            # (the cache)
            if df.columns.count(c) > 1:
                if c not in join_cols:
                    df_col = f'a.{c}'
                    cache_col = f'cache.{c}'
                    renamed_col = f'renamed_{c}'

                    df = df.withColumn(
                        renamed_col,
                        F.when(
                            F.col(df_col).isNull(), F.col(cache_col)
                        ).otherwise(F.col(df_col))
                    )
                    df = df.select(*[i for i in df.columns
                                     if i not in [cache_col, df_col, c]
                                     ])
                    df = df.withColumnRenamed(renamed_col, c)
        df.unpersist(blocking=True)
        return df

    def filter_by(self, df, columns=None):
        """

        :param df:
        :param columns:
        :return:
        """
        if not columns:
            columns = df.columns

        self.__cache = self.persistent_cache_file.join(
            df,
            on=columns,
            how='inner'
        ).drop(
            'a.ip'
        )

    def update_self(
            self,
            source_df,
            join_cols=('target', 'ip'),
            select_cols=('*',),
            expire=True
    ):
        """

        :param source_df:
        :param join_cols:
        :param select_cols:
        :param expire:
        :return:
        """
        # if os.path.exists(self.persistent_cache_file):
        #     shutil.rmtree(self.persistent_cache_file)
        #     # time.sleep(1)

        to_drop = [
            'prediction', 'r', 'score', 'to_update', 'id', 'id_runtime',
            'features', 'start', 'stop', 'subset_count', 'num_requests',
            'total_seconds', 'time_bucket', 'model_version', 'to_update',
            'label', 'id_attribute', 'id_request_sets', 'created_at',
            'dt', 'id_client'
        ]
        now = datetime.datetime.utcnow()
        #source_df = source_df.persist(self.storage_level)
        source_df = source_df.alias('sd')

        columns = source_df.columns
        columns.remove('first_ever_request')
        columns.remove('target_original')
        source_df = source_df.select(columns)

        # http://www.learnbymarketing.com/1100/pyspark-joins-by-example/
        self.__persistent_cache = source_df.rdd.toDF(source_df.schema).join(
            self.__persistent_cache.select(*select_cols).alias('pc'),
            list(join_cols),
            how='full_outer'
        )#.persist(self.storage_level)

        # mark rows to update
        self.__persistent_cache = self.__persistent_cache.withColumn(
            'to_update',
            F.col('features').isNotNull()
        )

        # update cache columns
        for cache_col, df_col in self.column_renamings.items():
            self.__persistent_cache = self.__persistent_cache.withColumn(
                cache_col,
                F.when(
                    F.col('to_update') == True, F.col(df_col)  # noqa
                ).otherwise(
                    F.when(
                        F.col(cache_col).isNotNull(),
                        F.col(cache_col)
                    )
                )
            )
        self.__persistent_cache = self.__persistent_cache.withColumn(
            'updated_at',
            F.when(
                F.col('to_update') == True, now  # noqa
            ).otherwise(F.col('updated_at'))
        )
        # drop cols that do not belong in the cache
        self.__persistent_cache = self.__persistent_cache.drop(*to_drop)

        # remove old rows
        if expire:
            update_date = now - datetime.timedelta(
                seconds=self.expire_if_longer_than
            )
            self.__persistent_cache = self.__persistent_cache.select(
                '*'
            ).where(F.col('updated_at') >= update_date)

        # we don't need anything in memory anymore
        source_df.unpersist(blocking=True)
        self.empty()

    def empty(self):
        if self.__cache is not None:
            self.__cache.unpersist(blocking=True)
        self.__cache = None
        gc.collect()
        self.session_getter().sparkContext._jvm.System.gc()

