# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import datetime
import itertools
import json
import os
import traceback

import pyspark
from pyspark.sql import functions as F, types as T
from pyspark.sql.types import StringType, StructField, StructType, DoubleType
from pyspark.streaming import StreamingContext
from functools import reduce
from pyspark.sql import DataFrame
import pyspark.sql.functions as psf

from baskerville.db import get_jdbc_url
from baskerville.db.models import RequestSet, Model
from baskerville.models.ip_cache import IPCache
from baskerville.models.pipeline_tasks.tasks_base import Task, MLTask, \
    CacheTask
from baskerville.models.config import BaskervilleConfig
from baskerville.spark.helpers import map_to_array, load_test, \
    save_df_to_table, columns_to_dict, get_window, set_unknown_prediction
from baskerville.spark.schemas import features_schema, \
    prediction_schema
from kafka import KafkaProducer
from dateutil.tz import tzutc

# broadcasts
from baskerville.util.helpers import instantiate_from_str, get_model_path

TOPIC_BC = None
KAFKA_URL_BC = None
CLIENT_MODE_BC = None
OUTPUT_COLS_BC = None


class GetDataKafka(Task):
    """
    Retrieves data from Kafka in batches of time_bucket seconds.
    For every batch, the configured steps are executed.
    """

    def __init__(
            self,
            config: BaskervilleConfig,
            steps: list = (),
            group_by_cols=('client_request_host', 'client_ip')
    ):
        super().__init__(config, steps)
        self.ssc = None
        self.kafka_stream = None
        self.group_by_cols = group_by_cols
        self.data_parser = self.config.engine.data_config.parser
        self.kafka_params = {
            'metadata.broker.list': self.config.kafka.bootstrap_servers,
            'auto.offset.reset': 'largest',
            'group.id': self.config.kafka.consume_group,
            'auto.create.topics.enable': 'true'
        }
        self.consume_topic = self.config.kafka.logs_topic

    def initialize(self):
        super(GetDataKafka, self).initialize()
        self.ssc = StreamingContext(
            self.spark.sparkContext, self.config.engine.time_bucket
        )
        from pyspark.streaming.kafka import KafkaUtils
        self.kafka_stream = KafkaUtils.createDirectStream(
            self.ssc,
            [self.consume_topic],
            kafkaParams=self.kafka_params,
        )

    def get_data(self):
        self.df = self.df.map(lambda l: json.loads(l[1])).toDF(
            self.data_parser.schema
        ).repartition(
            *self.group_by_cols
        ).persist(
            self.config.spark.storage_level
        )

        self.df = load_test(
            self.df,
            self.config.engine.load_test,
            self.config.spark.storage_level
        )

    def run(self):
        self.create_runtime()

        def process_subsets(time, rdd):
            self.logger.info(f'Data until {time} from kafka topic \'{self.consume_topic}\'')
            if not rdd.isEmpty():
                try:
                    # set dataframe to process later on
                    # todo: handle edge cases
                    # todo: what happens if we have a missing column here?
                    # todo: does the time this takes to complete affects the
                    # kafka messages consumption?
                    self.df = rdd
                    self.get_data()
                    if self.df.rdd.isEmpty():
                        self.logger.warning('Task.get_data() returned an empty dataframe.')
                        return
                    self.remaining_steps = list(self.step_to_action.keys())

                    super(GetDataKafka, self).run()

                    items_to_unpersist = self.spark.sparkContext._jsc. \
                        getPersistentRDDs().items()
                    self.logger.debug(
                        f'_jsc.getPersistentRDDs().items():'
                        f'{len(items_to_unpersist)}')
                    rdd.unpersist()
                    del rdd
                except Exception as e:
                    traceback.print_exc()
                    self.logger.error(e)
                finally:
                    self.reset()
            else:
                self.logger.info('Empty RDD...')

        self.kafka_stream.foreachRDD(process_subsets)

        self.ssc.start()
        self.ssc.awaitTermination()
        return self.df


class GetFeatures(GetDataKafka):
    """
    Listens to the prediction input topic on the ISAC side
    """

    def __init__(self, config: BaskervilleConfig, steps: list = ()):
        super().__init__(config, steps)
        self.consume_topic = self.config.kafka.features_topic

    def get_data(self):
        self.df = self.spark.createDataFrame(
            self.df,
            T.StructType([T.StructField('key', T.StringType()), T.StructField('message', T.StringType())])
        )

        schema = T.StructType([
            T.StructField("id_client", T.StringType(), True),
            T.StructField("id_request_sets", T.StringType(), False)
        ])
        features = T.StructType()
        for feature in self.config.engine.all_features.keys():
            features.add(T.StructField(
                name=feature,
                dataType=T.StringType(),
                nullable=True))
        schema.add(T.StructField("features", features))

        self.df = self.df.withColumn('message', F.from_json('message', schema))

        self.df = self.df \
            .withColumn('features', F.col('message.features')) \
            .withColumn('id_client', F.col('message.id_client')) \
            .withColumn('id_request_sets', F.col('message.id_request_sets')) \
            .drop('message', 'key')

        self.df = self.df.where(F.col("id_client").isNotNull())


class GetPredictions(GetDataKafka):
    """
    Listens to the prediction input topic on the client side
    """

    def __init__(self, config: BaskervilleConfig, steps: list = ()):
        super().__init__(config, steps)
        self.consume_topic = self.config.kafka.predictions_topic

    def get_data(self):
        self.df = self.df.map(lambda l: json.loads(l[1])).toDF(
            prediction_schema  # todo: dataparser.schema
        ).persist(
            self.config.spark.storage_level
        )
        # self.df.show()
        # json_schema = self.spark.read.json(
        #     self.df.limit(1).rdd.map(lambda row: row.features)
        # ).schema
        # self.df = self.df.withColumn(
        #     'features',
        #     F.from_json('features', json_schema)
        # )


class GetDataKafkaStreaming(Task):
    def __init__(self, config: BaskervilleConfig, steps: list = ()):
        super().__init__(config, steps)
        self.stream_df = None
        self.kafka_params = {
            'kafka.bootstrap.servers': self.config.bootstrap_servers,
            'metadata.broker.list': self.config.kafka.bootstrap_servers,
            'auto.offset.reset': 'largest',
            'group.id': self.config.kafka.consume_group,
            'auto.create.topics.enable': 'true',
            'partition.assignment.strategy': 'range'
        }

    def initialize(self):
        super(GetDataKafkaStreaming, self).initialize()
        self.stream_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config.kafka.bootstrap_servers) \
            .option("subscribe", self.config.kafka.predictions_topic) \
            .option("startingOffsets", "earliest")

    def get_data(self):
        self.stream_df = self.stream_df.load().selectExpr(
            "CAST(key AS STRING)", "CAST(value AS STRING)"
        )

    def run(self):
        self.create_runtime()
        self.get_data()
        self.df = self.stream_df.select(
            F.from_json(
                F.col("value").cast("string"),
                features_schema
            )
        )

        def process_row(row):
            print(row)
            # self.df = row
            # self.df = super(GetDataKafkaStreaming, self).run()

        self.df.writeStream.format(
            'console'
        ).foreach(
            process_row
        ).start().awaitTermination()

        return self.df


class GetDataLog(Task):
    """
    Reads json files.
    """

    def __init__(self, config, steps=(),
                 group_by_cols=('client_request_host', 'client_ip'), ):
        super().__init__(config, steps)
        self.log_paths = self.config.engine.raw_log.paths
        self.group_by_cols = group_by_cols
        self.batch_i = 1
        self.batch_n = len(self.log_paths)
        self.current_log_path = None

    def initialize(self):
        super().initialize()
        for step in self.steps:
            step.initialize()

    def create_runtime(self):
        self.runtime = self.tools.create_runtime(
            file_name=self.current_log_path,
            conf=self.config.engine,
            comment=f'batch runtime {self.batch_i} of {self.batch_n}'
        )
        self.logger.info('Created runtime {}'.format(self.runtime.id))

    def get_data(self):
        """
        Gets the dataframe according to the configuration
        :return: None
        """

        self.df = self.spark.read.json(
            self.current_log_path
        ).persist(
            self.config.spark.storage_level)

        self.logger.info('Got dataframe of #{} records'.format(
            self.df.count())
        )
        self.df = load_test(
            self.df,
            self.config.engine.load_test,
            self.config.spark.storage_level
        )

    def process_data(self):
        """
        Splits the data into time bucket length windows and executes all
        the steps
        :return:
        """
        if self.df.count() == 0:
            self.logger.info('No data in to process.')
        else:
            for window_df in get_window(
                    self.df, self.time_bucket, self.config.spark.storage_level
            ):
                self.df = window_df.repartition(
                    *self.group_by_cols
                ).persist(self.config.spark.storage_level)
                self.remaining_steps = list(self.step_to_action.keys())
                self.df = super().run()
                self.reset()

    def run(self):
        for log in self.log_paths:
            self.logger.info(f'Processing {log}...')
            self.current_log_path = log

            self.create_runtime()
            self.get_data()
            self.process_data()
            self.reset()

            self.batch_i += 1


class GetDataPostgres(Task):
    """
    Reads data from RequestSet's table in Postgres - used for training
    """

    def __init__(
            self,
            config: BaskervilleConfig,
            steps: list = (),
            columns_to_keep=('ip', 'target', 'stop', 'features',),
            from_date=None,
            to_date=None,
            training_days=None
    ):
        super().__init__(config, steps)
        self.columns_to_keep = columns_to_keep
        self.n_rows = -1
        self.from_date = from_date
        self.to_date = to_date
        self.training_days = training_days
        self.conn_properties = {
            'user': self.config.database.user,
            'password': self.config.database.password,
            'driver': self.config.spark.db_driver,
        }
        self.db_url = get_jdbc_url(self.config.database)

    def get_bounds(self, from_date, to_date=None, field='stop'):
        """
        Get the lower and upper limit
        :param str from_date: lower date bound
        :param str to_date: upper date bound
        :param str field: date field
        :return:
        """
        where = f'{field}>=\'{from_date}\' '
        if to_date:
            where += f'AND {field}<=\'{to_date}\' '
        q = f"(select min(id) as min_id, " \
            f"max(id) as max_id, " \
            f"count(id) as rows " \
            f"from request_sets " \
            f"where {where}) as bounds"
        return self.spark.read.jdbc(
            url=self.db_url,
            table=q,
            properties=self.conn_properties
        )

    def load(self) -> pyspark.sql.DataFrame:
        """
        Loads the request_sets already in the database
        :return:
        :rtype: pyspark.sql.Dataframe
        """
        to_date = self.to_date
        from_date = self.from_date
        if not from_date or not to_date:
            if self.training_days:
                to_date = datetime.datetime.utcnow()
                from_date = str(to_date - datetime.timedelta(
                    days=self.training_days
                ))
                to_date = str(to_date)
            else:
                raise ValueError(
                    'Please specify either from-to dates or training days'
                )

        bounds = self.get_bounds(from_date, to_date, field='stop').collect()[0]
        self.logger.debug(
            f'Fetching {bounds.rows} rows. '
            f'min: {bounds.min_id} max: {bounds.max_id}'
        )
        q = f'(select id, {",".join(self.columns_to_keep)} ' \
            f'from request_sets where id >= {bounds.min_id}  ' \
            f'and id <= {bounds.max_id} and stop >= \'{from_date}\' ' \
            f'and stop <=\'{to_date}\') as request_sets'

        return self.spark.read.jdbc(
            url=self.db_url,
            table=q,
            numPartitions=int(self.spark.conf.get(
                'spark.sql.shuffle.partitions'
            )) or os.cpu_count() * 2,
            column='id',
            lowerBound=bounds.min_id,
            upperBound=bounds.max_id + 1,
            properties=self.conn_properties
        )

    def run(self):
        self.df = self.load()
        self.df = super().run()
        return self.df


class GenerateFeatures(MLTask):
    def __init__(
            self,
            config,
            steps=(),
    ):
        super().__init__(config, steps)
        self.data_parser = self.config.engine.data_config.parser
        self.group_by_cols = list(set(
            self.config.engine.data_config.group_by_cols
        ))
        self.group_by_aggs = None
        self.post_group_by_aggs = None
        self.columns_to_filter_by = None
        self.drop_if_missing_filter = None
        self.cols_to_drop = None

    def initialize(self):
        MLTask.initialize(self)
        self.drop_if_missing_filter = self.data_parser.drop_if_missing_filter()

        # gather calculations
        self.group_by_aggs = self.get_group_by_aggs()
        self.columns_to_filter_by = self.get_columns_to_filter_by()
        self.cols_to_drop = set(
            self.feature_manager.active_feature_names +
            self.feature_manager.active_columns +
            list(self.group_by_aggs.keys()) +
            self.feature_manager.update_feature_cols
        ).difference(RequestSet.columns)

    def handle_missing_columns(self):
        """
        Check for missing columns and if any use the data parser to add them
        and fill them with defaults, if specified in the schema.
        :return:
        """
        missing = self.data_parser.check_for_missing_columns(self.df)
        if missing:
            self.df = self.data_parser.add_missing_columns(
                self.df, missing
            )

    def rename_columns(self):
        """
        Some column names may cause issues with spark, e.g. `geo.ip.lat`, so
        the features that use those can declare in `columns_renamed` that those
        columns should be renamed to something else, e.g. `geo_ip_lat`
        :return:
        """
        cols = self.df.columns
        for k, v in self.feature_manager.column_renamings:
            if k in cols:
                self.df = self.df.withColumnRenamed(k, v)
            else:
                self.df = self.df.withColumn(v, F.col(k))

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
        self.df = self.df.select(*self.columns_to_filter_by)
        if where is not None:
            self.df = self.df.where(where)

        # todo: metric for dropped logs
        print(f'{self.df.count()}')

    def handle_missing_values(self):
        self.df = self.data_parser.fill_missing_values(self.df)

    def normalize_host_names(self):
        """
        From www.somedomain.tld keep somedomain
        # todo: improve this and remove udf
        # todo: keep original target in a separate field in db
        :return:
        """
        from baskerville.spark.udfs import udf_normalize_host_name
        self.df = self.df.fillna({'client_request_host': ''})
        self.df = self.df.withColumn('target_original', F.col('client_request_host').cast(T.StringType()))
        self.df = self.df.withColumn(
            'client_request_host',
            udf_normalize_host_name(
                F.col('target_original')
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

        self.df = self.df.withColumn(
            '@timestamp', F.col('@timestamp').cast('timestamp')
        )

        for k, v in self.feature_manager.pre_group_by_calculations.items():
            self.df = self.df.withColumn(
                k, v
            )

        for f in self.feature_manager.active_features:
            self.df = f.misc_compute(self.df)

    def group_by(self):
        """
        Group the logs df by the given group-by columns (normally IP, host).
        :return: None
        """
        self.df = self.df.withColumn('ip', F.col('client_ip'))
        self.df = self.df.withColumn(
            'target', F.col('client_request_host')
        )
        self.df = self.df.groupBy(
            'ip', 'target'
        ).agg(
            *self.group_by_aggs.values()
        )

    def get_post_group_by_calculations(self):
        """
        Gathers the columns and computations to be performed after the grouping
        of the data (df)
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
        self.df = self.service_provider.add_cache_columns(self.df)

        for k, v in self.get_post_group_by_calculations().items():
            self.df = self.df.withColumn(k, v)

        self.df = self.df.drop('old_subset_count')

    def feature_extraction(self):
        """
        For each feature compute the feature value and add it as a column in
        the dataframe
        :return: None
        """

        for feature in self.feature_manager.active_features:
            self.df = feature.compute(self.df)

        self.logger.info(
            f'Number of logs after feature extraction {self.df.count()}'
        )
        # self.df = self.df.cache()

    def remove_feature_columns(self):
        self.df = self.df.drop(
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
        columns_to_gather = [
            f.feature_name for f in self.feature_manager.active_features
        ]
        self.df = columns_to_dict(self.df, 'features', columns_to_gather)
        self.df = columns_to_dict(self.df, 'old_features', columns_to_gather)
        self.df.persist(self.config.spark.storage_level)

        for f in self.feature_manager.updateable_active_features:
            self.df = f.update(self.df).cache()

        self.df = self.df.withColumn('features', F.create_map(
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
        self.df = map_to_array(
            self.df,
            'features',
            'vectorized_features',
            self.feature_manager.active_feature_names
        )
        self.remove_feature_columns()
        self.df = self.df.drop('old_features')

        self.df = self.df.withColumn(
            'subset_count',
            F.col('subset_count') + F.lit(1)
        )

        self.df = self.df.withColumn(
            'num_requests',
            F.when(
                F.col('old_num_requests') > 0,
                F.col('old_num_requests') + F.col('num_requests')
            ).otherwise(F.col('num_requests'))
        )
        self.df = self.df.drop('old_num_requests')
        diff = (F.unix_timestamp('last_request', format="YYYY-MM-DD %H:%M:%S")
                - F.unix_timestamp(
                    'start', format="YYYY-MM-DD %H:%M:%S")
                ).cast('float')
        self.df = self.df.withColumn('total_seconds', diff)
        self.df = self.df.drop(*self.cols_to_drop)

    def feature_calculation(self):
        """
        Add calculation cols, extract features, and update.
        :return:
        """
        self.add_post_groupby_columns()
        self.feature_extraction()
        self.feature_update()

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
        cols = self.group_by_cols + self.feature_manager.active_columns + ['target_original']
        cols.append(self.config.engine.data_config.timestamp_column)
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
            'num_requests': F.count(F.col('@timestamp')).alias('num_requests'),
            'target_original': F.first(F.col('target_original')).alias('target_original')
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

    def add_ids(self):
        self.df = self.df.withColumn(
            'id_client', F.lit(self.config.engine.id_client)
        ).withColumn(
            'id_request_sets', F.monotonically_increasing_id()
        ).withColumn(
            'id_request_sets',
            F.concat_ws(
                '_',
                F.col('id_client'),
                F.col('id_request_sets'),
                F.col('start').cast('long').cast('string'))
        )
        # todo: monotonically_increasing_id guarantees uniqueness within
        #  the current batch, this will cause conflicts with caching - use
        # e.g. the timestamp too to avoid this

    def run(self):
        self.handle_missing_columns()
        self.normalize_host_names()
        self.rename_columns()
        self.filter_columns()
        self.handle_missing_values()
        self.add_calc_columns()
        self.group_by()
        self.feature_calculation()
        self.add_ids()

        return super().run()


class Predict(MLTask):
    """
    Adds prediction and score columns, given a features column
    """

    def __init__(self, config: BaskervilleConfig, steps=()):
        super().__init__(config, steps)
        self._can_predict = False
        self._is_initialized = False

    def predict(self):
        if self.model:
            self.df = self.model.predict(self.df)
        else:
            self.df = set_unknown_prediction(self.df).withColumn(
                'prediction', F.col('prediction').cast(T.IntegerType())
            ).withColumn(
                'score', F.col('score').cast(T.FloatType())
            ).withColumn(
                'threshold', F.col('threshold').cast(T.FloatType()))

            self.logger.error('No model to predict')

    def run(self):
        self.predict()
        self.df = super(Predict, self).run()
        return self.df


class SaveDfInPostgres(Task):
    def __init__(
            self,
            config,
            steps=(),
            table_name=RequestSet.__tablename__,
            json_cols=('features',),
            mode='append'
    ):
        super().__init__(config, steps)
        self.table_name = table_name
        self.json_cols = json_cols
        self.mode = mode

    def run(self):
        self.config.database.conn_str = self.db_url

        save_df_to_table(
            self.df,
            self.table_name,
            self.config.database.__dict__,
            json_cols=self.json_cols,
            storage_level=self.config.spark.storage_level,
            mode=self.mode,
            db_driver=self.config.spark.db_driver
        )
        self.df = super().run()
        return self.df


class Save(SaveDfInPostgres):
    """
    Saves dataframe in Postgres (current backend)
    """

    def prepare_to_save(self):
        request_set_columns = RequestSet.columns[:]
        not_common = {
            'prediction', 'model_version', 'label', 'id_attribute',
            'updated_at'
        }.difference(self.df.columns)

        for c in not_common:
            request_set_columns.remove(c)

        if len(self.df.columns) < len(request_set_columns):
            # log and let it blow up; we need to know that we cannot save
            self.logger.error(
                'The input df columns are different than '
                'the actual table columns'
            )

        self.df = self.df.select(request_set_columns)
        self.df = self.df.withColumn(
            'created_at',
            F.current_timestamp()
        )

    def run(self):
        self.prepare_to_save()
        # save request_sets
        self.logger.debug('Saving request_sets')
        self.df = super().run()
        return self.df


class RefreshCache(CacheTask):
    def run(self):
        self.service_provider.refresh_cache(self.df)
        return super().run()


class CacheSensitiveData(Task):
    def __init__(
            self,
            config,
            steps=(),
            table_name=RequestSet.__tablename__,
    ):
        super().__init__(config, steps)
        self.table_name = table_name
        self.ttl = self.config.engine.ttl

    def run(self):
        self.df = self.df.drop('vectorized_features')
        redis_df = self.df.withColumn("features", F.to_json("features"))

        redis_df = redis_df.withColumn('start', F.date_format(F.col('start'), 'yyyy-MM-dd HH:mm:ss')) \
            .withColumn('stop', F.date_format(F.col('stop'), 'yyyy-MM-dd HH:mm:ss'))

        redis_df.write.format(
            'org.apache.spark.sql.redis'
        ).mode(
            'append'
        ).option(
            'table', self.table_name
        ).option(
            'ttl', self.ttl
        ).option(
            'key.column', 'id_request_sets'
        ).save()
        self.df = super().run()
        return self.df


class MergeWithSensitiveData(Task):
    def __init__(
            self,
            config,
            steps=(),
            table_name=RequestSet.__tablename__,
    ):
        super().__init__(config, steps)
        self.redis_df = None
        self.table_name = table_name

    def run(self):
        self.redis_df = self.spark.read.format(
            'org.apache.spark.sql.redis'
        ).option(
            'table', self.table_name
        ).option(
            'key.column', 'id_request_sets'
        ).load().alias('redis_df')

        self.redis_df = self.redis_df.withColumn('start', F.to_timestamp(F.col('start'), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn('stop', F.to_timestamp(F.col('stop'), "yyyy-MM-dd HH:mm:ss"))

        self.df = self.df.alias('df')
        count = self.df.count()
        self.df = self.redis_df.join(
            self.df, on=['id_client', 'id_request_sets']
        ).drop('df.id_client', 'df.id_request_sets')

        if count != self.df.count():
            self.logger.warning(f'Failed to retrieve {count - self.df.count()} records from Redis')

        self.df = super().run()
        return self.df


class SendToKafka(Task):
    def __init__(
            self,
            config: BaskervilleConfig,
            columns,
            topic,
            cc_to_client=False,
            steps: list = (),
    ):
        super().__init__(config, steps)
        self.columns = columns
        self.topic = topic
        self.cc_to_client = cc_to_client

    def run(self):
        self.logger.info(f'Sending to kafka topic \'{self.topic}\'...')

        producer = KafkaProducer(bootstrap_servers=self.config.kafka.bootstrap_servers)
        records = self.df.collect()
        for record in records:
            message = json.dumps(
                {key: record[key] for key in self.columns}
            ).encode('utf-8')
            producer.send(self.topic, message)
            if self.cc_to_client:
                id_client = record['id_client']
                producer.send(f'{self.topic}.{id_client}', message)
            producer.flush()

        # does no work, possible jar conflict
        # self.df = self.df.select(
        #         F.col('id_client').alias('key'),
        #         F.to_json(
        #             F.struct([self.df[x] for x in self.df.columns])
        #         ).alias('value')
        #     ) \
        #     .write \
        #     .format('kafka') \
        #     .option('kafka.bootstrap.servers', self.config.kafka.bootstrap_servers) \
        #     .option('topic', self.topic) \
        #     .save()
        return self.df


class Train(Task):

    def __init__(
            self,
            config: BaskervilleConfig,
            steps: list = (),
    ):
        super().__init__(config, steps)
        self.model = None
        self.training_conf = self.config.engine.training
        self.engine_conf = self.config.engine

    def initialize(self):
        super().initialize()

    def load_dataset(self, df, features):
        dataset = df.persist(self.spark_conf.storage_level)

        if self.training_conf.max_samples_per_host:
            counts = dataset.groupby('target').count()
            counts = counts.withColumn('fraction', self.training_conf.max_samples_per_host / F.col('count'))
            fractions = dict(counts.select('target', 'fraction').collect())
            for key, value in fractions.items():
                if value > 1.0:
                    fractions[key] = 1.0
            dataset = dataset.sampleBy('target', fractions, 777)

        schema = StructType([])
        for feature in features:
            schema.add(StructField(
                name=feature,
                dataType=StringType(),
                nullable=True))

        dataset = dataset.withColumn(
            'features',
            F.from_json('features', schema)
        )
        for feature in features:
            column = f'features.{feature}'
            feature_class = self.engine_conf.all_features[feature]
            dataset = dataset.withColumn(column, F.col(column).cast(feature_class.spark_type()).alias(column))

        self.logger.debug(f'Loaded {dataset.count()} rows dataset...')
        return dataset

    def save(self):
        """
        Save the models on disc and add a baskerville.db.Model in the database
        :return: None
        """
        model_path = get_model_path(
            self.engine_conf.storage_path, self.model.__class__.__name__)
        self.model.save(path=model_path, spark_session=self.spark)
        self.logger.debug(f'The new model has been saved to: {model_path}')

        db_model = Model()
        db_model.created_at = datetime.datetime.now(tz=tzutc())
        db_model.algorithm = self.training_conf.model
        db_model.parameters = json.dumps(self.model.get_params())
        db_model.classifier = bytearray(model_path.encode('utf8'))

        # save to db
        self.db_tools.session.add(db_model)
        self.db_tools.session.commit()

    def run(self):

        self.model = instantiate_from_str(self.training_conf.model)

        params = self.training_conf.model_parameters

        model_features = {}
        for feature in params['features']:
            features_class = self.engine_conf.all_features[feature]
            model_features[feature] = {
                'categorical': features_class.is_categorical(),
                'string': features_class.spark_type() == StringType()
            }
        params['features'] = model_features

        self.model.set_params(**params)
        self.model.set_logger(self.logger)

        dataset = self.load_dataset(self.df, self.model.features)

        self.model.train(dataset)
        dataset.unpersist()

        self.save()


class Evaluate(Task):
    pass


class SaveFeaturesTileDb(MLTask):
    """
    Saves dataframe in TileDb
    """

    def save(self):
        from baskerville.util.helpers import get_default_data_path
        df = self.df
        for f_name in self.feature_manager.active_feature_names:
            df = df.withColumn(f_name, F.col('features').getItem(f_name))
        df.select(
            'id_request_sets',
            'prediction',
            'score',
            'stop',
            *self.feature_manager.active_feature_names
        ).write.format('io.tiledb.spark').option(
            'uri', f'{get_default_data_path()}/tiledbstorage'
        ).option(
            'schema.dim.0.name', 'id_request_sets'
        ).save()

    def run(self):
        self.logger.debug('Saving features...')
        self.save()
        self.df = super().run()
        return self.df


class SaveFeaturesHbase(MLTask):
    """
    Saves dataframe in Hbase
    """

    def __init__(self, config, steps=()):
        super().__init__(config, steps)
        self.catalog = {
            'table': {'namespace': 'default', 'name': 'request_sets'},
            'rowkey': 'id_request_sets',
            'columns': {
                'id_request_sets': {'cf': 'rowkey', 'col': 'id_request_sets', 'type': 'string'},
                'prediction': {'cf': 'cf1', 'col': 'prediction', 'type': 'int'},
                'score': {'cf': 'cf1', 'col': 'score', 'type': 'double'},
                'stop': {'cf': 'cf1', 'col': 'stop', 'type': 'timestamp'},
            }
        }

    def initialize(self):
        import json
        for i, f_name in enumerate(self.feature_manager.active_feature_names):
            self.catalog[f_name] = {
                'cf': 'cf1',
                'col': f_name,
                'type': 'double'
            }
        self.catalog = json.dumps(self.catalog)

    def save(self):
        df = self.df
        for f_name in self.feature_manager.active_feature_names:
            df = df.withColumn(f_name, F.col('features').getItem(f_name))
        df.select(
            'id_request_sets',
            'prediction',
            'score',
            'stop',
            *self.feature_manager.active_feature_names
        ).write.options(
            catalog=self.catalog
        ).format("org.apache.spark.sql.execution.datasources.hbase").save()

    def run(self):
        self.save()


class SaveFeaturesHive(MLTask):
    """
    Saves dataframe in Hive
    """

    def save(self):
        from baskerville.util.helpers import get_default_data_path
        df = self.df
        for f_name in self.feature_manager.active_feature_names:
            df = df.withColumn(f_name, F.col('features').getItem(f_name))
        df.select(
            'id_request_sets',
            'prediction',
            'score',
            'stop',
            *self.feature_manager.active_feature_names
        ).write.format('io.tiledb.spark').option(
            'uri', f'{get_default_data_path()}/tiledbstorage'
        ).option(
            'schema.dim.0.name', 'id_request_sets'
        ).save()

    def run(self):
        self.logger.debug('Saving features...')
        self.save()
        self.df = super().run()
        return self.df


class AttackDetection(Task):
    """
    Calculates prediction per IP, attack_score per Target, regular vs anomaly counts, attack_prediction
    """
    collected_df_attack = None

    def __init__(self, config, steps=()):
        super().__init__(config, steps)
        self.df_chunks = []
        self.df_white_list = None
        self.ip_cache = IPCache(self.logger, self.config.engine.ip_cache_ttl)

    def initialize(self):
        # super(SaveStats, self).initialize()
        self.set_metrics()
        self.df_white_list = self.spark.createDataFrame(
            [[ip] for ip in self.config.engine.white_list], ['ip']).withColumn('white_list', F.lit(1))

    def classify_anomalies(self):
        self.logger.info('Anomaly thresholding...')
        self.df = self.df.withColumn('prediction',
                                     F.when(F.col('score') > self.config.engine.anomaly_threshold,
                                            F.lit(1.0)).otherwise(F.lit(0.)))

    def update_sliding_window(self):
        self.logger.info('Updating sliding window...')
        df_increment = self.df.select('target', 'stop', 'prediction') \
            .withColumn('stop', F.to_timestamp(F.col('stop'), "yyyy-MM-dd HH:mm:ss"))

        increment_stop = df_increment.groupby().agg(F.max('stop')).collect()[0].asDict()['max(stop)']
        self.logger.info(f'max_ts= {increment_stop}')

        df_increment = self.df.select('target', 'stop', 'prediction'). \
            withColumn('stop', F.to_timestamp(F.col('stop'), 'yyyy-MM-dd HH:mm:ss')).groupBy('target').agg(
            F.count('prediction').alias('total'),
            F.max('stop').alias('ts'),
            F.sum(F.when(F.col('prediction') == 0, F.lit(1)).otherwise(F.lit(0))).alias('regular'),
            F.sum(F.when(F.col('prediction') > 0, F.lit(1)).otherwise(F.lit(0))).alias('anomaly')
        ).persist(self.config.spark.storage_level)

        if len(self.df_chunks) > 0 and self.df_chunks[0][1] < increment_stop - datetime.timedelta(
                seconds=self.config.engine.sliding_window):
            self.logger.info(f'Removing sliding window tail at {self.df_chunks[0][1]}')
            self.df_chunks.pop()

        total_size = df_increment.count()
        for chunk in self.df_chunks:
            total_size += chunk[0].count()
        self.df_chunks.append((df_increment, increment_stop))
        self.logger.info(f'Sliding window size {total_size}...')

    def get_attack_score(self):
        chunks = [c[0] for c in self.df_chunks]
        df = reduce(DataFrame.unionAll, chunks).groupBy('target').agg(
            F.sum('total').alias('total'),
            F.sum('regular').alias('regular'),
            F.sum('anomaly').alias('anomaly'),
        )

        df = df.withColumn('attack_score', F.col('anomaly').cast('float') / F.col('total').cast('float')) \
            .persist(self.config.spark.storage_level)

        return df

    def detect_low_rate_attack(self, df):
        schema = T.StructType()
        schema.add(StructField(name='request_total', dataType=StringType(), nullable=True))
        df = df.withColumn('f', F.from_json('features', schema))
        df = df.withColumn('f.request_total', F.col('f.request_total').cast(DoubleType()).alias('f.request_total'))

        df_attackers = df.filter(
            (F.col('f.request_total') > self.config.engine.low_rate_attack_period) &
            ((psf.abs(psf.unix_timestamp(df.stop)) - psf.abs(psf.unix_timestamp(df.start))) >
             self.config.engine.low_rate_attack_total_request)
        ).select('ip', 'target', 'f.request_total', 'start').withColumn('low_rate_attack', F.lit(1))

        if df_attackers.count() > 0:
            self.logger.info(f'Low rate attack -------------- {df_attackers.count()} ips')
            self.logger.info(df_attackers.show())

        df = df.join(df_attackers.select('ip', 'low_rate_attack'), on='ip', how='left')
        return df

    def apply_white_list(self, df):
        df = df.join(self.df_white_list, on='ip', how='left')
        white_listed = df.where((F.col('white_list') == 1) & (F.col('prediction') == 1))
        if white_listed.count() > 0:
            self.logger.info(f'White listing {white_listed.count()} ips')
            self.logger.info(white_listed.select('ip').show())

        df = df.withColumn('attack_prediction', F.when(
            (F.col('white_list') == 1), F.lit(0)).otherwise(F.col('attack_prediction')))
        df = df.withColumn('prediction', F.when(
            (F.col('white_list') == 1), F.lit(0)).otherwise(F.col('prediction')))
        df = df.withColumn('low_rate_attack', F.when(
            (F.col('white_list') == 1), F.lit(0)).otherwise(F.col('low_rate_attack')))
        return df

    def detect_attack(self):
        if self.config.engine.attack_threshold == 0:
            self.logger.info('Attack threshold is 0. No sliding window')
            df_attack = self.df[['target']].distinct()\
                .withColumn('attack_prediction', F.lit(1))\
                .withColumn('attack_score', F.lit(1))
            self.df = self.df.withColumn('attack_prediction', F.lit(1))
        else:
            self.update_sliding_window()
            df_attack = self.get_attack_score()

            df_attack = df_attack.withColumn('attack_prediction', F.when(
                (F.col('attack_score') > self.config.engine.attack_threshold) &
                (F.col('total') > self.config.engine.minimum_number_attackers), F.lit(1)).otherwise(F.lit(0)))

            self.df = self.df.join(df_attack.select(['target', 'attack_prediction']), on='target', how='left')

        self.df = self.detect_low_rate_attack(self.df)
        self.df = self.apply_white_list(self.df)
        return df_attack

    def set_metrics(self):
        # Perhaps using an additional label and one metric could be better
        # than two - performan ce-wise. But that will add another dimension
        # in Prometheus
        from baskerville.models.metrics.registry import metrics_registry
        from baskerville.util.enums import MetricClassEnum
        from baskerville.models.metrics.helpers import set_attack_score, \
            set_attack_prediction, set_attack_threshold

        run = metrics_registry.register_action_hook(
            self.run,
            set_attack_score,
            metric_cls=MetricClassEnum.gauge,
            metric_name='attack_score',
            labelnames=['target']
        )

        run = metrics_registry.register_action_hook(
            run,
            set_attack_prediction,
            metric_cls=MetricClassEnum.gauge,
            metric_name='attack_prediction',
            labelnames=['target']
        )

        run = metrics_registry.register_action_hook(
            run,
            set_attack_threshold,
            metric_cls=MetricClassEnum.gauge,
            metric_name='baskerville_config',
            labelnames=['value']
        )

        setattr(self, 'run', run)
        self.logger.info('Attack score metric set')

    def send_challenge(self, df_attack):
        producer = KafkaProducer(bootstrap_servers=self.config.kafka.bootstrap_servers)
        if self.config.engine.challenge == 'host':
            df_host_challenge = df_attack.where(F.col('attack_prediction') == 1)
            df_host_challenge = df_host_challenge.select('target').distinct().join(
                self.df.select('target', 'target_original').distinct(), on='target', how='left')

            records = df_host_challenge.select('target_original').distinct().collect()
            num_records = len(records)
            if num_records > 0:
                self.logger.info(
                    f'Sending {num_records} HOST challenge commands to kafka '
                    f'topic \'{self.config.kafka.banjax_command_topic}\'...')
                for record in records:
                    message = json.dumps(
                        {'name': 'challenge_host', 'value': record['target_original']}
                    ).encode('utf-8')
                    producer.send(self.config.kafka.banjax_command_topic, message)
                    producer.flush()
        elif self.config.engine.challenge == 'ip':
            ips = self.df.select(['ip']).where(
                (F.col('attack_prediction') == 1) & (F.col('prediction') == 1) |
                (F.col('low_rate_attack') == 1)
            )
            ips = self.ip_cache.filter(ips)
            records = ips.collect()
            num_records = len(records)
            if num_records > 0:
                self.logger.info(
                    f'Sending {num_records} IP challenge commands to '
                    f'kafka topic \'{self.config.kafka.banjax_command_topic}\'...')
                for record in records:
                    message = json.dumps(
                        {'name': 'challenge_ip', 'value': record['ip']}
                    ).encode('utf-8')
                    producer.send(self.config.kafka.banjax_command_topic, message)
                    producer.flush()
                self.ip_cache.add(ips)

    def run(self):
        self.classify_anomalies()
        df_attack = self.detect_attack()
        self.send_challenge(df_attack)

        self.collected_df_attack = df_attack.collect()

        self.df = super().run()
        return self.df
