# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import json
import datetime
import itertools
import os

from baskerville.models.base import PipelineBase
from baskerville.models.feature_manager import FeatureManager
from baskerville.spark.helpers import save_df_to_table, reset_spark_storage, set_unknown_prediction
from baskerville.spark.schemas import get_cache_schema
from baskerville.util.helpers import TimeBucket, FOLDER_CACHE, instantiate_from_str, load_model_from_path
from pyspark.sql import types as T, DataFrame

from baskerville.spark import get_or_create_spark_session
from dateutil.tz import tzutc
from collections import OrderedDict

from baskerville.util.baskerville_tools import BaskervilleDBTools
from baskerville.util.enums import Step

from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

from baskerville.db.models import RequestSet
from baskerville.models.request_set_cache import RequestSetSparkCache


class SparkPipelineBase(PipelineBase):
    """
    The base class for all pipelines that use spark. It initializes spark
    session and provides basic implementation for some of the main methods
    """

    def __init__(self,
                 db_conf,
                 engine_conf,
                 spark_conf,
                 clean_up=True,
                 group_by_cols=('client_request_host', 'client_ip'),
                 *args,
                 **kwargs
                 ):

        super().__init__(db_conf, engine_conf, clean_up)
        self.start_time = datetime.datetime.utcnow()
        self.request_set_cache = None
        self.spark = None
        self.tools = None
        self.spark_conf = spark_conf
        self.data_parser = self.engine_conf.data_config.parser
        self.group_by_cols = list(set(group_by_cols))
        self.group_by_aggs = None
        self.post_group_by_aggs = None
        self.columns_to_filter_by = None
        self._can_predict = False
        self._is_initialized = False
        self.drop_if_missing_filter = None
        self.cols_to_drop = None
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
            'user': self.db_conf.user,
            'password': self.db_conf.password
        }
        self.step_to_action = OrderedDict(
            zip([
                Step.preprocessing,
                Step.group_by,
                Step.feature_calculation,
                Step.label_or_predict,
                Step.trigger_challenge,
                Step.save
            ], [
                self.preprocessing,
                self.group_by,
                self.feature_calculation,
                self.label_or_predict,
                self.trigger_challenge,
                self.save
            ]))

        self.remaining_steps = list(self.step_to_action.keys())

        self.time_bucket = TimeBucket(self.engine_conf.time_bucket)
        self.feature_manager = FeatureManager(self.engine_conf)
        self.model_index = None
        self.model = None

    def load_test(self):
        """
        If the user has set the load_test configuration, then multiply the
        traffic by `self.engine_conf.load_test` times to do load testing.
        :return:
        """
        if self.engine_conf.load_test:
            df = self.logs_df.persist(self.spark_conf.storage_level)

            for i in range(self.engine_conf.load_test - 1):
                df = df.withColumn(
                    'client_ip', F.round(F.rand(42)).cast('string')
                )
                self.logs_df = self.logs_df.union(df).persist(
                    self.spark_conf.storage_level
                )

            df.unpersist()
            del df
            self.logger.info(
                f'---- Count after multiplication: {self.logs_df.count()}'
            )

    def reset(self):
        """
        Unpersist rdds and dataframes and call GC - see broadcast memory
        release issue
        :return:
        """
        import gc

        reset_spark_storage()
        gc.collect()

    def initialize(self):
        """
        Set the basics:
        - Connect to the database
        - Initialize spark session
        - Get active model and scaler and set them to broadcast variables
        - Get active features with their active columns, update columns etc and
        set the relevant broadcast variables
        - Set the _can_predict flag
        - Instantiate the accumulators (for metrics)
        - Instantiate request set cache.
        :return:
        """

        # initialize db access tools
        self.tools = BaskervilleDBTools(self.db_conf)
        self.tools.connect_to_db()

        # initialize spark session
        self.spark = self.instantiate_spark_session()
        self.feature_manager.initialize()
        self.drop_if_missing_filter = self.data_parser.drop_if_missing_filter()

        # set up cache
        self.request_set_cache = self.set_up_request_set_cache()

        # gather calculations
        self.group_by_aggs = self.get_group_by_aggs()
        self.columns_to_filter_by = self.get_columns_to_filter_by()
        self.cols_to_drop = set(
            self.feature_manager.active_feature_names +
            self.feature_manager.active_columns +
            list(self.group_by_aggs.keys()) +
            self.feature_manager.update_feature_cols
        ).difference(RequestSet.columns)

        if self.engine_conf.model_id:
            self.model_index = self.tools.get_ml_model_from_db(
                self.engine_conf.model_id)
            self.model = instantiate_from_str(self.model_index.algorithm)
            self.model.load(bytes.decode(
                self.model_index.classifier, 'utf8'), self.spark)
        elif self.engine_conf.model_path:
            self.model = load_model_from_path(self.engine_conf.model_path, self.spark)
        else:
            self.model = None

        self._is_initialized = True

    def get_columns_to_filter_by(self):
        """
        Gathers all the columns that need to be present in the dataframe
        for the processing to complete.
        group_by_cols: the columns to group data on
        active_columns: the columns that the active features have declared as
        necessary
        timestamp_column: the time column - all logs need to have a time column
        :return: a set of the column names that need to be present in the
        dataframe
        :rtype: set[str]
        """
        cols = self.group_by_cols + self.feature_manager.active_columns
        cols.append(self.engine_conf.data_config.timestamp_column)
        return set(cols)

    def get_group_by_aggs(self):
        """
        Gathers all the group by arguments:
        basic_aggs:
            - first_request
            - last_request
            - num_requests
        column_aggs: the columns the features need for computation are gathered
         as lists
        feature_aggs: the columns the features need for computation
        Priority: basic_aggs > feature_aggs > column_aggs
        The basic aggs have a priority over the feature and column aggs.
        The feature aggs have a priority over the column aggs (if a feature
        has explicitly asked for a computation for a specific column it relies
        upon, then the computation will be stored instead of the column
        aggregation as list)

        :return: a dictionary with the name of the group by aggregation columns
        as keys and the respective Column aggregation as values
        :rtype: dict[string, pyspark.Column]
        """
        basic_aggs = {
            'first_request': F.min(F.col('@timestamp')).alias('first_request'),
            'last_request': F.max(F.col('@timestamp')).alias('last_request'),
            'num_requests': F.count(F.col('@timestamp')).alias('num_requests')
        }

        column_aggs = {
            c: F.collect_list(F.col(c)).alias(c)
            for c in self.feature_manager.active_columns
        }

        feature_aggs = self.feature_manager.get_feature_group_by_aggs()

        basic_aggs.update(
            {k: v for k, v in feature_aggs.items() if k not in basic_aggs}
        )
        basic_aggs.update(
            {k: v for k, v in column_aggs.items() if k not in basic_aggs}
        )

        return basic_aggs

    def get_post_group_by_calculations(self):
        """
        Gathers the columns and computations to be performed after the grouping
        of the data (logs_df)
        Basic post group by columns:
        - `id_runtime`
        - `time_bucket`
        - `start`
        - `stop`
        - `subset_count`

        if there is an ML Model defined:
        - `model_version`
        - `classifier`
        - `scaler`
        - `model_features`

        Each feature can also define post group by calculations using the
        post_group_by_calcs dict.

        :return: A dictionary with the name of the result columns as keys and
        their respective computations as values
        :rtype: dict[string, pyspark.Column]
        """
        if self.post_group_by_aggs:
            return self.post_group_by_aggs

        post_group_by_columns = {
            'id_runtime': F.lit(self.runtime.id),
            'time_bucket': F.lit(self.time_bucket.sec),
            'start': F.when(
                F.col('first_ever_request').isNotNull(),
                F.col('first_ever_request')
            ).otherwise(F.col('first_request')),
            'stop': F.col('last_request'),
            'subset_count': F.when(
                F.col('old_subset_count').isNotNull(),
                F.col('old_subset_count')
            ).otherwise(F.lit(0))
        }

        if self.model_index:
            post_group_by_columns['model_version'] = F.lit(
                self.model_index.id
            )

        # todo: what if a feature defines a column name that already exists?
        # e.g. like `subset_count`
        post_group_by_columns.update(
            self.feature_manager.post_group_by_calculations
        )

        return post_group_by_columns

    def __getitem__(self, name):
        if name == 'run':
            if not self._is_initialized:
                raise RuntimeError(
                    f'__getitem__: {self.__class__.__name__} '
                    f'has not been initialized yet.'
                )
        return getattr(self, name)

    def __getattribute__(self, name):
        if name == 'run':
            if not self._is_initialized:
                raise RuntimeError(
                    f'__getattribute__:{self.__class__.__name__} '
                    f'has not been initialized yet.'
                )

        return super().__getattribute__(name)

    def filter_cache(self):
        """
        Use the current logs to find the past request sets - if any - in the
        request set cache
        :return:
        """
        df = self.logs_df.select(
            F.col('client_request_host').alias('target'),
            F.col('client_ip').alias('ip'),
        ).distinct().alias('a').persist(self.spark_conf.storage_level)

        self.request_set_cache.filter_by(df)

        df.unpersist()
        del df

    def run(self):
        """
        Runs the configured steps.
        :return:
        """
        self.logger.info(
            f'Spark UI accessible at:{self.spark.sparkContext.uiWebUrl}'
        )
        self.create_runtime()
        self.get_data()
        self.process_data()

    def process_data(self):
        """
        Splits the data into time bucket length windows and executes all the steps
        :return:
        """
        if self.logs_df.count() == 0:
            self.logger.info('No data in to process.')
        else:
            for window_df in self.get_window():
                self.logs_df = window_df
                self.logs_df = self.logs_df.repartition(
                    *self.group_by_cols
                ).persist(self.spark_conf.storage_level)
                self.remaining_steps = list(self.step_to_action.keys())

                for step, action in self.step_to_action.items():
                    self.logger.info('Starting step {}'.format(step))
                    action()
                    self.logger.info('Completed step {}'.format(step))
                    self.remaining_steps.remove(step)
                self.reset()

    def get_window(self):

        from pyspark.sql import functions as F

        df = self.logs_df.withColumn('timestamp',
                                     F.col('@timestamp').cast('timestamp'))
        df = df.sort('timestamp')
        current_window_start = df.agg({"timestamp": "min"}).collect()[0][0]
        stop = df.agg({"timestamp": "max"}).collect()[0][0]
        window_df = None
        current_end = current_window_start + self.time_bucket.td

        while True:
            if window_df:
                window_df.unpersist(blocking=True)
                del window_df
            filter_ = (
                (F.col('timestamp') >= current_window_start) &
                (F.col('timestamp') < current_end)
            )
            window_df = df.where(filter_).persist(
                self.spark_conf.storage_level
            )
            if not window_df.rdd.isEmpty():
                print(f'# Request sets = {window_df.count()}')
                yield window_df
            else:
                self.logger.info(f'Empty window df for {str(filter_._jc)}')
            current_window_start = current_window_start + self.time_bucket.td
            current_end = current_window_start + self.time_bucket.td
            if current_window_start >= stop:
                window_df.unpersist(blocking=True)
                del window_df
                break

    def create_runtime(self):
        """
        Create a Runtime in the Baskerville database.
        :return:
        """
        raise NotImplementedError(
            'SparkPipelineBase does not have an implementation '
            'for _create_runtime.'
        )

    def get_data(self):
        """
        Get dataframe of log data
        :return:
        """
        raise NotImplementedError(
            'SparkPipelineBase does not have an implementation '
            'for _get_data.'
        )

    def preprocessing(self):
        """
        Fill missing values, add calculation cols, and filter.
        :return:
        """

        self.handle_missing_columns()
        self.rename_columns()
        self.filter_columns()
        self.handle_missing_values()
        self.normalize_host_names()
        self.add_calc_columns()

    def group_by(self):
        """
        Group the logs df by the given group-by columns (normally IP, host).
        :return: None
        """
        self.logs_df = self.logs_df.groupBy(
            *self.group_by_cols
        ).agg(
            *self.group_by_aggs.values()
        )

    def feature_calculation(self):
        """
        Add calculation cols, extract features, and update.
        :return:
        """
        self.add_post_groupby_columns()
        self.feature_extraction()
        self.feature_update()

    def label_or_predict(self):
        """
        Apply label from MISP or predict label.
        #todo: use separate steps for this
        :return:
        """

        from pyspark.sql import functions as F
        from pyspark.sql.types import IntegerType

        if self.engine_conf.cross_reference:
            self.cross_reference()
        else:
            self.logs_df = self.logs_df.withColumn(
                'label', F.lit(None).cast(IntegerType()))
        self.predict()

    def get_challenges(self, df, challenge_threshold):
        def challenge_decision(num_normals, num_anomalies, threshold):
            if num_anomalies >= threshold * (num_anomalies + num_normals):
                return 1
            return 0

        challenge_decision_udf = udf(challenge_decision, IntegerType())

        df = df.select(['target', 'prediction'])\
            .groupBy(['target', 'prediction'])\
            .count()\
            .groupBy('target')\
            .pivot('prediction').agg(F.first('count'))\
            .withColumn('challenge', challenge_decision_udf(F.col('0'), F.col('1'), F.lit(challenge_threshold)))\
            .select(['target', 'challenge'])
        return []

    def send_challenges(self, challenges):
        pass

    def save_challenges_to_db(self, challenges):
        pass

    def trigger_challenge(self):
        """
        Trigger the challenge per host
        :return:
        """
        # if not self.engine_conf.trigger_challenge:
        #     return
        #
        # challenges = self.get_challenges(self.logs_df, self.engine_conf.challenge_threshold)
        # if len(challenges):
        #     self.send_challenges(challenges)
        #     self.save_challenges_to_db(challenges)

    def save(self):
        """
        Update dataframe, save to database, and update cache.
        :return:
        """
        request_set_columns = RequestSet.columns[:]
        not_common = {
            'prediction', 'model_version', 'label', 'id_attribute',
            'updated_at'
        }.difference(self.logs_df.columns)

        for c in not_common:
            request_set_columns.remove(c)

        # filter the logs df with the request_set columns
        self.logs_df = self.logs_df.select(request_set_columns)

        # save request_sets
        self.logger.debug('Saving request_sets')
        self.save_df_to_table(
            self.logs_df.select(request_set_columns),
            RequestSet.__tablename__
        )
        self.refresh_cache()

    def refresh_cache(self):
        """
        Update the cache with the current batch of logs in logs_df and clean up
        :return:
        """
        self.request_set_cache.update_self(self.logs_df)
        self.logs_df.unpersist()
        self.logs_df = None
        # self.spark.catalog.clearCache()

    def finish_up(self):
        """
        Try to gracefully stop by committing to database, emptying the cache,
        unpersist everything, disconnecting from db and clearing spark's cache
        :return: None
        """
        if len(self.remaining_steps) == 0:
            request_set_count = self.tools.session.query(RequestSet).filter(
                RequestSet.id_runtime == self.runtime.id).count()
            self.runtime.processed = True
            self.runtime.n_request_sets = request_set_count
            self.tools.session.commit()
            self.logger.debug('finished updating runtime')
        try:
            self.request_set_cache.empty()
        except AttributeError:
            pass

        if hasattr(self, 'logs_df') and self.logs_df and isinstance(
                self.logs_df, DataFrame):
            self.logs_df.unpersist(blocking=True)
            self.logs_df = None

        if self.tools and self.tools.session:
            self.tools.session.commit()
            self.tools.disconnect_from_db()
        if self.spark:
            try:
                if self.spark_conf.spark_python_profile:
                    self.spark.sparkContext.show_profiles()
                reset_spark_storage()
            except AttributeError:
                self.logger.debug('clearCache attr error')
                pass

    def instantiate_spark_session(self):
        """
        #todo
        :return:
        """
        return get_or_create_spark_session(self.spark_conf)

    def set_up_request_set_cache(self):
        """
        Set up an instance of RequestSetSparkCache using the cache
        configuration. Also, load past (start_time - expiry_time)
        if cache_load_past is configured.
        :return:
        """
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
            expire_if_longer_than=self.engine_conf.cache_expire_time,
            path=os.path.join(self.engine_conf.storage_path, FOLDER_CACHE)
        )
        if self.engine_conf.cache_load_past:
            self.request_set_cache = self.request_set_cache.load(
                update_date=(
                    self.start_time - datetime.timedelta(
                        seconds=self.engine_conf.cache_expire_time
                    )
                ).replace(tzinfo=tzutc()),
                extra_filters=(
                    F.col('time_bucket') == self.time_bucket.sec
                )  # todo: & (F.col("id_runtime") == self.runtime.id)?
            )
        else:
            self.request_set_cache.load_empty(get_cache_schema())

        self.logger.info(f'In cache: {self.request_set_cache.count()}')

        return self.request_set_cache

    def handle_missing_columns(self):
        """
        Check for missing columns and if any use the data parser to add them
        and fill them with defaults, if specified in the schema.
        :return:
        """
        missing = self.data_parser.check_for_missing_columns(self.logs_df)
        if missing:
            self.logs_df = self.data_parser.add_missing_columns(
                self.logs_df, missing
            )

    def rename_columns(self):
        """
        Some column names may cause issues with spark, e.g. `geo.ip.lat`, so
        the features that use those can declare in `columns_renamed` that those
        columns should be renamed to something else, e.g. `geo_ip_lat`
        :return:
        """
        for k, v in self.feature_manager.column_renamings:
            self.logs_df = self.logs_df.withColumnRenamed(k, v)

    def filter_columns(self):
        """
        Logs df may have columns that are not necessary for the analysis,
        filter them out to reduce the memory footprint.
        The absolutely essential columns are the group by columns and the
        timestamp column, or else the rest of the process will fail.
        And of course the columns the features need, the active columns.
        :return:None
        """

        where = self.drop_if_missing_filter
        self.logs_df = self.logs_df.select(*self.columns_to_filter_by)
        if where is not None:
            self.logs_df = self.logs_df.where(where)

        # todo: metric for dropped logs
        print(f'{self.logs_df.count()}')

    def handle_missing_values(self):
        self.logs_df = self.data_parser.fill_missing_values(self.logs_df)

    def normalize_host_names(self):
        """
        From www.somedomain.tld keep somedomain
        # todo: improve this and remove udf
        # todo: keep original target in a separate field in db
        :return:
        """
        from baskerville.spark.udfs import udf_normalize_host_name

        self.logs_df = self.logs_df.withColumn(
            'client_request_host',
            udf_normalize_host_name(
                F.col('client_request_host').cast(T.StringType())
            )
        )

    def add_calc_columns(self):
        """
        Each feature needs different calculations in order to be able to
        compute the feature value. Go through the features and apply the
        calculations. Each calculation can occur only once, calculations
        with the same name will be ignored.
        :return:
        """

        self.logs_df = self.logs_df.withColumn(
            '@timestamp', F.col('@timestamp').cast('timestamp')
        )

        for k, v in self.feature_manager.pre_group_by_calculations.items():
            self.logs_df = self.logs_df.withColumn(
                k, v
            )

        for f in self.feature_manager.active_features:
            self.logs_df = f.misc_compute(self.logs_df)

    def add_cache_columns(self):
        """
        Add columns from the cache to facilitate the
        feature extraction, prediction, and save processes
        :return:
        """
        self.logs_df = self.logs_df.alias('logs_df')
        self.filter_cache()
        self.logs_df = self.request_set_cache.update_df(
            self.logs_df, select_cols=self.cache_columns
        )
        self.logger.debug(
            f'****** > # of rows in cache: {self.request_set_cache.count()}')

    def add_post_groupby_columns(self):
        """
        Add extra columns after the grouping of the logs to facilitate the
        feature extraction, prediction, and save processes
        Extra columns:
        * general:
        ----------
        - ip
        - target
        - id_runtime
        - time_bucket
        - start
        - subset_count

        * cache columns:
        ----------------
        - 'id',
        - 'first_ever_request',
        - 'old_subset_count',
        - 'old_features',
        - 'old_num_requests'

        * model related:
        ----------------
        - model_version
        - classifier
        - scaler
        - model_features

        :return: None
        """
        # todo: shouldn't this be a renaming?
        self.logs_df = self.logs_df.withColumn('ip', F.col('client_ip'))
        self.logs_df = self.logs_df.withColumn(
            'target', F.col('client_request_host')
        )
        self.add_cache_columns()

        for k, v in self.get_post_group_by_calculations().items():
            self.logs_df = self.logs_df.withColumn(k, v)

        self.logs_df = self.logs_df.drop('old_subset_count')

    def feature_extraction(self):
        """
        For each feature compute the feature value and add it as a column in
        the dataframe
        :return: None
        """

        for feature in self.feature_manager.active_features:
            self.logs_df = feature.compute(self.logs_df)

        self.logger.info(
            f'Number of logs after feature extraction {self.logs_df.count()}'
        )
        # self.logs_df = self.logs_df.cache()

    def remove_feature_columns(self):
        self.logs_df = self.logs_df.drop(
            *self.feature_manager.active_feature_names
        )

    def feature_update(self):
        """
        Update current batch's features with past features - if any - using
        the request set cache.
        :return:
        """
        # convert current features to dict since the already saved request_sets
        # have the features as json
        self.features_to_dict('features')
        self.features_to_dict('old_features')
        self.logs_df.persist(self.spark_conf.storage_level)

        for f in self.feature_manager.updateable_active_features:
            self.logs_df = f.update(self.logs_df).cache()

        self.logs_df = self.logs_df.withColumn('features', F.create_map(
            *list(
                itertools.chain(
                    *[
                        (F.lit(f.feature_name),
                         F.col(f.updated_feature_col_name))
                        for f in
                        self.feature_manager.updateable_active_features
                    ]
                )
            )
        ))

        # older way with a udf:
        # self.logs_df = self.logs_df.withColumn(
        #     'features',
        #     udf_update_features(
        #         F.lit(cPickle.dumps([  # #WST
        #             StrippedFeature(f.feature_name, f.update_row)
        #             for f in self.feature_manager.active_features
        #         ]
        #         )),
        #         'features',
        #         'old_features',
        #         'subset_count',
        #         'start',
        #         'last_request'
        #     )
        # ).persist(self.spark_conf.storage_level)
        self.logs_df = self.logs_df.drop('old_features')

        self.logs_df = self.logs_df.withColumn(
            'subset_count',
            F.col('subset_count') + F.lit(1)
        )

        self.logs_df = self.logs_df.withColumn(
            'num_requests',
            F.when(
                F.col('old_num_requests') > 0,
                F.col('old_num_requests') + F.col('num_requests')
            ).otherwise(F.col('num_requests'))
        )
        self.logs_df = self.logs_df.drop('old_num_requests')
        diff = (F.unix_timestamp('last_request', format="YYYY-MM-DD %H:%M:%S")
                - F.unix_timestamp(
                    'start', format="YYYY-MM-DD %H:%M:%S")
                ).cast('float')
        self.logs_df = self.logs_df.withColumn('total_seconds', diff)
        self.logs_df = self.logs_df.drop(*self.cols_to_drop)

    def cross_reference(self):
        """
        Look up IPs in attributes table, and label as malicious (-1) if listed
        there.
        :return:
        """
        from pyspark.sql import functions as F
        from baskerville.spark.udfs import udf_cross_reference_misp

        self.logs_df = self.logs_df.withColumn(
            'cross_reference',
            udf_cross_reference_misp('ip',
                                     F.lit(json.dumps(self.db_conf.__dict__)))
        )
        self.logs_df = self.logs_df.withColumn(
            'label',
            F.when(F.col('cross_reference.label') != 0,
                   F.col('cross_reference.label')).otherwise(None)
        )
        self.logs_df = self.logs_df.withColumn(
            'id_attribute',
            F.when(F.col('cross_reference.id_attribute') != 0,
                   F.col('cross_reference.id_attribute')).otherwise(None)
        )

    def predict(self):
        """
        Predict on the request_sets. Prediction on request_sets
        requires feature averaging where there is an existing request_set.
`        :return: None
        """
        if self.model:
            self.logs_df = self.model.predict(self.logs_df)
        else:
            self.logs_df = set_unknown_prediction(self.logs_df).withColumn(
                'prediction', F.col('prediction').cast(T.IntegerType())
            ).withColumn(
                'score', F.col('score').cast(T.FloatType())
            ).withColumn(
                'threshold', F.col('threshold').cast(T.FloatType()))

    def save_df_to_table(
            self, df, table_name, json_cols=('features',), mode='append'
    ):
        """
        Save the dataframe to the database. Jsonify any columns that need to
        be
        :param pyspark.Dataframe df: the dataframe to save
        :param str table_name: where to save the dataframe
        :param iterable json_cols: the name of the columns that need to be
        jsonified, e.g. `features`
        :param str mode: save mode, 'append', 'overwrite' etc, see spark save
        options
        :return: None
        """
        # todo: mostly duplicated code, refactor
        self.db_conf.conn_str = self.db_url
        save_df_to_table(
            df,
            table_name,
            self.db_conf.__dict__,
            json_cols=json_cols,
            storage_level=self.spark_conf.storage_level,
            mode=mode,
            db_driver=self.spark_conf.db_driver
        )

    def features_to_dict(self, column):
        """
        Convert the features to dictionary
        :param string column: the name of the column to use as output
        :return: None
        """
        from pyspark.sql import functions as F

        self.logs_df = self.logs_df.withColumn(
            column,
            F.create_map(
                *list(
                    itertools.chain(
                        *[
                            (F.lit(f.feature_name), F.col(f.feature_name))
                            for f in self.feature_manager.active_features
                        ]
                    )
                ))
        )
