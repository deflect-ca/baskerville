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
from pyspark.streaming import StreamingContext

from baskerville.db.models import RequestSet
from baskerville.models.pipeline_tasks.tasks_base import Task, MLTask, \
    CacheTask
from baskerville.models.config import BaskervilleConfig
from baskerville.spark.helpers import map_to_array, load_test, \
    save_df_to_table, columns_to_dict, get_window, set_unknown_prediction
from baskerville.spark.schemas import features_schema, \
    prediction_schema
from kafka import KafkaProducer

# broadcasts
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
            columns_to_keep=('ip', 'target', 'created_at', 'features',)
    ):
        super().__init__(config, steps)
        self.columns_to_keep = columns_to_keep
        self.n_rows = -1
        self.conn_properties = {
            'user': self.config.database.user,
            'password': self.config.database.password,
            'driver': self.config.database.db_driver,
        }

    def get_data(self):
        """
        Load the data from the database into a dataframe and do the necessary
        transformations to get the features as a list \
        :return:
        """
        self.df = self.load().persist(self.config.spark.storage_level)

        # since features are stored as json, we need to expand them to create
        # vectors
        json_schema = self.spark.read.json(
            self.df.limit(1).rdd.map(lambda row: row.features)
        ).schema
        self.df = self.df.withColumn(
            'features',
            F.from_json('features', json_schema)
        )

        # get the active feature names and transform the features to list
        self.active_features = json_schema.fieldNames()
        data = map_to_array(
            self.df,
            'features',
            'features',
            self.active_features
        ).persist(self.spark_conf.storage_level)
        self.df.unpersist()
        self.df = data
        self.n_rows = self.df.count()
        self.logger.debug(f'Loaded #{self.n_rows} of request sets...')
        return self.df

    def get_bounds(self, from_date, to_date=None, field='created_at'):
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

    def load(self, extra_filters=None) -> pyspark.sql.DataFrame:
        """
        Loads the request_sets already in the database
        :return:
        :rtype: pyspark.sql.Dataframe
        """
        data_params = self.config.engine.training.data_parameters
        from_date = data_params.get('from_date')
        to_date = data_params.get('to_date')
        training_days = data_params.get('training_days')

        if training_days:
            to_date = datetime.datetime.utcnow()
            from_date = str(to_date - datetime.timedelta(
                days=training_days
            ))
            to_date = str(to_date)
        if not training_days and (not from_date or not to_date):
            raise ValueError(
                'Please specify either from-to dates or training days'
            )

        bounds = self.get_bounds(from_date, to_date).collect()[0]
        self.logger.debug(
            f'Fetching {bounds.rows} rows. '
            f'min: {bounds.min_id} max: {bounds.max_id}'
        )
        if not bounds.min_id:
            raise RuntimeError(
                'No data to train. Please, check your training configuration'
            )
        q = f'(select id, {",".join(self.columns_to_keep)} ' \
            f'from request_sets where id >= {bounds.min_id}  ' \
            f'and id <= {bounds.max_id} and created_at >= \'{from_date}\' ' \
            f'and created_at <=\'{to_date}\') as request_sets'

        if not extra_filters:
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
        raise NotImplementedError('No implementation for "extra_filters"')

    def run(self):
        self.df = self.get_data()
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
        self.df = self.df.withColumn(
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
        cols = self.group_by_cols + self.feature_manager.active_columns
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

        # filter the logs df with the request_set columns
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
        self.df = self.redis_df.join(
            self.df, on=['id_client', 'id_request_sets']
        ).drop('df.id_client', 'df.id_request_sets')

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
    pass


class Evaluate(Task):
    pass


class ModelUpdate(MLTask):
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
    Calculates attack score, regular vs irregular request sets and applies
    alert threshold
    """
    # note: this will increase memory consumption a bit
    collected_df = None

    def initialize(self):
        # super(SaveStats, self).initialize()
        self.set_metrics()

    def calculate_attack_score(self):
        """
        Based on:
        SELECT $__timeGroup(created_at, '2m'), target,
        avg(case when score >= 40/100.0 then 100.0 else 0 end) as anomalies
        FROM request_sets WHERE $__timeFilter(created_at)
        group by 1, 2
        HAVING COUNT(created_at) > 50
        order by 1
        Another way to do this is to use the average of scores and if it is
        over the challenge_threshold then set the average, else set 0
        """

        self.df = self.df.select(
            'target', 'score'
        ).withColumn(
            'score',
            F.when(
                F.col('score') > self.config.engine.anomaly_threshold,
                F.lit(1.0)
            ).otherwise(F.lit(0.))
        ).withColumn(
            'regular_rs',
            F.when(F.col('score') > 0, F.lit(1)).otherwise(F.lit(0))
        ).withColumn(
            'irregular_rs',
            F.when(F.col('score') == 0, F.lit(1)).otherwise(F.lit(0))
        ).persist(self.config.spark.storage_level)
        self.df = self.df.groupBy('target').agg(
            F.count('score').alias('count'),
            F.sum('regular_rs').alias('regular_rs'),
            F.sum('irregular_rs').alias('irregular_rs'),
            F.avg('score').alias('avg_score')
        ).where(
            F.col('count') > self.config.engine.min_num_requests
        ).persist(self.config.spark.storage_level)

        self.df = self.df.withColumn(
            'attack_score',
            F.when(
                F.col('avg_score') > self.config.engine.challenge_threshold,
                F.col('avg_score')
            ).otherwise(F.lit(0))
        )

        return self.df

    def set_challenge_flag(self):
        """
        When attack score > 0 set challenge to True
        """
        self.df = self.df.withColumn('challenge', F.col('attack_score') > 0)
        return self.df

    def set_metrics(self):
        # Perhaps using an additional label and one metric could be better
        # than two - performance-wise. But that will add another dimension
        # in Prometheus
        from baskerville.models.metrics.registry import metrics_registry
        from baskerville.util.enums import MetricClassEnum
        from baskerville.models.metrics.helpers import set_attack_score, \
            set_topmost_metric

        run = metrics_registry.register_action_hook(
            self.run,
            set_topmost_metric,
            metric_cls=MetricClassEnum.histogram,
            metric_name='topmost',
            labelnames=['target', 'kind']
        )
        run = metrics_registry.register_action_hook(
            run,
            set_attack_score,
            metric_cls=MetricClassEnum.histogram,
            metric_name='attack_score',
            labelnames=['target']
        )
        setattr(self, 'run', run)
        self.logger.info('Attack score metric set')

    def run(self):
        # filter the logs df with the request_set columns
        # todo: reduce columns, not all are necessary - check next steps
        self.df = self.df.select(
            'id_request_sets', 'target', 'features', 'prediction', 'score'
        )
        self.logger.info('Calculating anomaly score...')
        self.calculate_attack_score()
        self.set_challenge_flag()

        self.df = super().run()
        return self.df
