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
            format_='parquet',
            path='request_set_cache'
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
        self.file_manager = FileManager(path, self.session_getter())

        self.file_name = os.path.join(
            path, f'{self.__class__.__name__}.{self.format_}')
        self.temp_file_name = os.path.join(
            path, f'{self.__class__.__name__}temp.{self.format_}')

        if self.file_manager.path_exists(self.file_name):
            self.file_manager.delete_path(self.file_name)
        if self.file_manager.path_exists(self.temp_file_name):
            self.file_manager.delete_path(self.temp_file_name)

    @property
    def cache(self):
        return self.__cache

    @property
    def persistent_cache(self):
        return self.__persistent_cache

    @property
    def persistent_cache_file(self):
        return self.file_name

    def _get_load_q(self):
        return f'''(SELECT *
                    from {self.table_name}
                    where id in (select max(id)
                    from {self.table_name}
                    group by {', '.join(self.group_by_fields)} )
                    ) as {self.table_name}'''

    def options(self, **kwargs):
        self.options = kwargs
        return self

    def _load(self, update_date=None, hosts=None, extra_filters=None):
        """
        Loads the request_sets already in the database
        :return:
        :rtype: pyspark.sql.Dataframe
        """
        where = ()
        if update_date:
            where = (
                    (F.col("updated_at") >= update_date) |
                    (F.col("created_at") >= update_date)
            )
        if not isinstance(where, F.Column):
            where = (F.col('id').isNotNull())

        if isinstance(extra_filters, F.Column):
            where = where & extra_filters

        spark = self.session_getter()

        df = spark.read.format('jdbc').options(
            url=self.cache_config['db_url'],
            driver=self.cache_config['db_driver'],
            dbtable=self._get_load_q(),
            user=self.cache_config['user'],
            password=self.cache_config['password'],
            fetchsize=1000,
            max_connections=200,
        ).load(
        ).where(
            where
        ).select(
            *self.columns_to_keep
        )

        if hosts is not None:
            df = df.join(F.broadcast(hosts), ['target'], 'leftsemi')

        self._changed = True

        return df

    def write(self):
        """
        Write persistent cache to file
        :return: None
        """
        self.__cache.write.mode('overwrite').partitionBy(
            *self.group_by_fields
        ).format(self.format_).save(self.persistent_cache_file)

    def load(self, update_date=None, hosts=None, extra_filters=None):
        """
        Load cache from database and store it in the configured file format
        :param update_date:
        :param hosts:
        :param extra_filters:
        :return:
        """
        self.__cache = self._load(
            update_date=update_date,
            hosts=hosts,
            extra_filters=extra_filters
        ).persist(self.storage_level)

        self.write()

        return self

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

    def append(self, df):
        self.__cache = self.__cache.union(
            df.select(*self.columns_to_keep)
        )

        return self

    def update_cache(self, df):
        self.__cache = self.__cache.select('*').where(
            F.col('id') not in df.select('id').distinct()
        )

        self.__cache = self.append(df)

        return self

    def update_df(
            self, df_to_update, join_cols=('target', 'ip'), select_cols=('*',)
    ):
        self._changed = True

        if "*" in select_cols:
            select_cols = self.cache.columns

        # add null columns if nothing in cache
        if self.count() == 0:
            for c in select_cols:
                if c not in df_to_update.columns:
                    df_to_update = df_to_update.withColumn(c, F.lit(None))
            return df_to_update

        # https://issues.apache.org/jira/browse/SPARK-10925
        df = df_to_update.rdd.toDF(df_to_update.schema).alias('a').join(
            F.broadcast(self.cache.select(*select_cols).alias('cache')),
            list(join_cols),
            how='left_outer'
        ).persist(self.storage_level)

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
        import os
        if not columns:
            columns = df.columns

        if os.path.isdir(self.persistent_cache_file):
            self.__cache = self.session_getter().read.format(
                self.format_
            ).load(self.persistent_cache_file).join(
                F.broadcast(df),
                on=columns,
                how='inner'
            ).drop(
                'a.ip'
            ).persist(self.storage_level)
        else:
            if self.__cache:
                self.__cache = self.__cache.join(
                    F.broadcast(df),
                    on=columns,
                    how='inner'
                ).drop(
                    'a.ip'
                ).persist(self.storage_level)
            else:
                self.load_empty(self.schema)

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
            'prediction', 'r', 'score', 'to_update', 'id', 'id_runtime', 'features',
            'start', 'stop', 'subset_count', 'num_requests', 'total_seconds',
            'time_bucket', 'model_version', 'to_update', 'label', 'id_attribute'
        ]
        now = datetime.datetime.utcnow()
        source_df = source_df.persist(self.storage_level).alias('sd')

        self.logger.debug(f'Source_df count = {source_df.count()}')

        # read the whole thing again
        if self.file_manager.path_exists(self.file_name):
            self.__persistent_cache = self.session_getter().read.format(
                self.format_
            ).load(
                self.file_name
            ).persist(self.storage_level)

        # http://www.learnbymarketing.com/1100/pyspark-joins-by-example/
        self.__persistent_cache = F.broadcast(
            source_df.rdd.toDF(source_df.schema)
        ).join(
            self.__persistent_cache.select(*select_cols).alias('pc'),
            list(join_cols),
            how='full_outer'
        ).persist(self.storage_level)

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

        # write back to parquet - different file/folder though
        # because self.parquet_name is already in use
        # rename temp to self.parquet_name
        if self.file_manager.path_exists(self.temp_file_name):
            self.file_manager.delete_path(self.temp_file_name)

        self.__persistent_cache.write.mode(
            'overwrite'
        ).format(
            self.format_
        ).save(self.temp_file_name)

        self.logger.debug(
            f'# Number of rows in persistent cache: '
            f'{self.__persistent_cache.count()}'
        )

        # we don't need anything in memory anymore
        source_df.unpersist(blocking=True)
        source_df = None
        del source_df
        self.empty_all()

        # rename temp to self.parquet_name
        if self.file_manager.path_exists(self.file_name):
            self.file_manager.delete_path(self.file_name)

        self.file_manager.rename_path(self.temp_file_name, self.file_name)

    def refresh(self, update_date, hosts, extra_filters=None):
        df = self._load(
            update_date=update_date, hosts=hosts, extra_filters=extra_filters
        )

        self.append(
            df
        ).deduplicate()

        return self

    def deduplicate(self):
        self.__cache = self.__cache.dropDuplicates()
        # self._count = self.cache.count()
        # self._last_updated = datetime.datetime.now()
        # self._changed = False

    def alias(self, name):
        self.__cache = self.__cache.alias(name)
        return self

    def show(self, n=20, t=False):
        self.__cache.show(n, t)

    def select(self, what):
        return self.__cache.select(what)

    def count(self):
        if self.__cache:
            try:
                return self.cache.count()
            except Py4JJavaError:
                import traceback
                traceback.print_exc()
                self.logger.debug(
                    'Just hit the cache issue.. trying to refresh')
                # self.cache.createOrReplaceTempView("current_cache")
                # self.session_getter().catalog.refreshTable("current_cache")
                return self.cache.count()

        return 0

    def clean(self, now, expire_if_longer_than=3600):
        """
        Remove request_sets with seconds since update > expire_if_longer_than
        :param datetime.datetime now: utc datetime
        :param int expire_if_longer_than: seconds
        :param bool allow_null: allow request_sets with null update_date (newly
        created)
        :return: None
        """
        update_date = now - datetime.timedelta(seconds=expire_if_longer_than)
        self.__cache = self.__cache.select('*').where((
            (F.col("updated_at") >= F.lit(update_date)) |
            (F.col("created_at") >= F.lit(update_date))
        ))

        return self

    def empty(self):
        if self.__cache is not None:
            self.__cache.unpersist(blocking=True)
        self.__cache = None

    def empty_all(self):
        if self.__cache is not None:
            self.__cache.unpersist(blocking=True)
        if self.__persistent_cache is not None:
            self.__persistent_cache.unpersist(blocking=True)

        self.__cache = None
        self.__persistent_cache = None
        gc.collect()
        self.session_getter().sparkContext._jvm.System.gc()

    def persist(self):
        self.__cache = self.__cache.persist(self.storage_level)
        # self.__cache.createOrReplaceTempView(self.__class__.__name__)
        # spark = self.session_getter()
        # spark.catalog.cacheTable(self.__class__.__name__)

    def __len__(self):
        return self.count()
