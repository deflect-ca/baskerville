# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import datetime
import os

from dateutil.tz import tzutc
from pyspark.sql import functions as F

from baskerville.db import get_jdbc_url
from baskerville.db.models import RequestSet
from baskerville.models.config import BaskervilleConfig
from baskerville.models.feature_manager import FeatureManager
from baskerville.models.request_set_cache import RequestSetSparkCache
from baskerville.spark.helpers import reset_spark_storage
from baskerville.spark.schemas import rs_cache_schema
from baskerville.util.helpers import Borg, TimeBucket, get_logger, \
    instantiate_from_str, FOLDER_CACHE, load_model_from_path


class ServiceProvider(Borg):
    """
    Provides the following services:
    - spark session
    - request set cache
    - db connection / db tools
    - ml related stuff: model, model index, feature manager
    """

    def __init__(self, config: BaskervilleConfig):
        self.config = config
        self.start_time = datetime.datetime.utcnow()
        self.runtime = None
        self.request_set_cache = None
        self.spark = None
        self.tools = None
        self.request_set_cache = None
        self._model = None
        self._model_ts = None
        self.model_index = None
        self.feature_manager = None
        self.spark_conf = self.config.spark
        self.db_url = get_jdbc_url(self.config.database)
        self.time_bucket = TimeBucket(self.config.engine.time_bucket)

        self.cache_columns = [
            'target',
            'ip',
            'first_ever_request',
            'old_subset_count',
            'old_features',
            'old_num_requests',
        ]
        self.cache_config = {
            'db_url': self.db_url,
            'db_driver': self.spark_conf.db_driver,
            'user': self.config.database.user,
            'password': self.config.database.password
        }
        self.logger = get_logger(
            self.__class__.__name__,
            logging_level=self.config.engine.log_level,
            output_file=self.config.engine.logpath
        )

    @property
    def model(self):
        return self._model

    def refresh_model(self):
        if self.config.engine.model_id and self._model_ts and \
                (datetime.datetime.utcnow() - self._model_ts).total_seconds() > \
                self.config.engine.new_model_check_in_seconds:
            self.load_model_from_db()

    def create_runtime(self):
        self.runtime = self.tools.create_runtime(
            start=self.start_time,
            conf=self.config.engine
        )
        self.logger.info(f'Created runtime {self.runtime.id}')

    def initialize_spark_service(self):
        if not self.spark:
            from baskerville.spark import get_or_create_spark_session
            self.spark = get_or_create_spark_session(self.spark_conf)

    def initialize_request_set_cache_service(self):
        if not isinstance(self.request_set_cache, RequestSetSparkCache):
            self.request_set_cache = RequestSetSparkCache(
                cache_config=self.cache_config,
                table_name=RequestSet.__tablename__,
                columns_to_keep=(
                    'target',
                    'ip',
                    F.col('start').alias('first_ever_request'),
                    F.col('subset_count').alias('old_subset_count'),
                    F.col('features').alias('old_features'),
                    F.col('num_requests').alias('old_num_requests'),
                    F.col('updated_at')
                ),
                expire_if_longer_than=self.config.engine.cache_expire_time,
                path=os.path.join(self.config.engine.storage_path,
                                  FOLDER_CACHE),
                logger=self.logger,
                use_storage=self.config.engine.use_storage_for_request_cache
            )
            if self.config.engine.cache_load_past:
                self.request_set_cache = self.request_set_cache.load(
                    update_date=(self.start_time - datetime.timedelta(
                        seconds=self.config.engine.cache_expire_time)
                                 ).replace(tzinfo=tzutc()),
                    extra_filters=(
                            F.col('time_bucket') == self.time_bucket.sec
                    )  # todo: & (F.col("id_runtime") == self.runtime.id)?
                )
            else:
                self.request_set_cache.load_empty(rs_cache_schema)

            return self.request_set_cache

    def initialize_db_tools_service(self):
        if not self.tools:
            from baskerville.util.baskerville_tools import BaskervilleDBTools
            self.tools = BaskervilleDBTools(self.config.database)
            self.tools.connect_to_db()

    def load_model_from_db(self):
        new_model_index = self.tools.get_ml_model_from_db(
            self.config.engine.model_id)

        # Do not reload the same model
        if self.model_index and self.model_index.id == new_model_index.id:
            return

        self.model_index = new_model_index
        self._model = instantiate_from_str(self.model_index.algorithm)
        path = bytes.decode(self.model_index.classifier, 'utf8')
        self._model.load(path, self.spark)
        self.model_index.can_predict = self.feature_manager.feature_config_is_valid()
        self._model.set_logger(self.logger)
        self._model_ts = datetime.datetime.utcnow()
        self.logger.info(f'New model (id={self.model_index.id}) is loaded from {path}')

    def initialize_model_service(self):
        if not self._model:
            if self.config.engine.model_id:
                self.load_model_from_db()
            elif self.config.engine.model_path:
                self._model = load_model_from_path(
                    self.config.engine.model_path, self.spark
                )
                self._model.set_logger(self.logger)
            else:
                self._model = None

    def initialize_feature_manager_service(self):
        if not self.feature_manager:
            self.feature_manager = FeatureManager(self.config.engine)
            self.feature_manager.initialize()

    def initalize_ml_services(self):
        self.initialize_feature_manager_service()
        self.initialize_model_service()

    def refresh_cache(self, df):
        """
        Update the cache with the current batch of logs in logs_df and clean up
        :return:
        """
        self.request_set_cache.update_self(df)
        df.unpersist()
        df = None
        # self.spark.catalog.clearCache()

    def filter_cache(self, df):
        """
        Use the current logs to find the past request sets - if any - in the
        request set cache
        :return:
        """
        df = df.select(
            F.col('target'),
            F.col('ip'),
        ).distinct().alias('a')  # .persist(self.spark_conf.storage_level)

        self.request_set_cache.filter_by(df)

        df.unpersist()
        del df

    def add_cache_columns(self, df):
        """
        Add columns from the cache to facilitate the
        feature extraction, prediction, and save processes
        :return:
        :rtype: pyspark.sql.DataFrame
        """
        df = df.alias('df')
        self.filter_cache(df)
        df = self.request_set_cache.update_df(
            df, select_cols=self.cache_columns
        )
        return df

    def finish_up(self):
        """
        Unpersist all
        :return:
        """
        reset_spark_storage()

    def reset(self):
        """
        Unpersist rdds and dataframes and call GC - see broadcast memory
        release issue
        :return:
        """
        import gc

        reset_spark_storage()
        gc.collect()
