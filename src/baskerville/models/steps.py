import datetime
import itertools
import json
import os
import traceback

import pyspark
from pyspark.sql import functions as F, types as T
from pyspark.streaming import StreamingContext

from baskerville.db.models import RequestSet
from baskerville.models.base import Task, MLTask
from baskerville.models.config import BaskervilleConfig
from baskerville.spark.helpers import map_to_array, load_test, \
    save_df_to_table, columns_to_dict, get_window
from baskerville.spark.schemas import prediction_schema


class GetDataKafka(Task):
    def __init__(self, config: BaskervilleConfig, steps: list = ()):
        super().__init__(config, steps)
        self.data_parser = self.config.engine.data_config.parser
        self.kafka_params = {
            # 'bootstrap.servers': self.kafka_conf.bootstrap_servers,
            'metadata.broker.list': self.config.kafka.bootstrap_servers,
            'auto.offset.reset': 'largest',
            'group.id': self.config.kafka.consume_group,
            'auto.create.topics.enable': 'true'
        }

    def initialize(self):
        super(GetDataKafka, self).initialize()
        self.ssc = StreamingContext(
            self.spark.sparkContext, self.config.engine.time_bucket
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

        from pyspark.streaming.kafka import KafkaUtils
        kafkaStream = KafkaUtils.createDirectStream(
            self.ssc,
            [self.config.kafka.consume_topic],
            kafkaParams=self.kafka_params,
        )

        def process_subsets(time, rdd):
            global CLIENT_PREDICTION_ACCUMULATOR, CLIENT_REQUEST_SET_COUNT

            self.logger.info('Data until {}'.format(time))
            if not rdd.isEmpty():
                try:
                    # set dataframe to process later on
                    # todo: handle edge cases
                    # todo: what happens if we have a missing column here?
                    # todo: does the time this takes to complete affects the
                    # kafka messages consumption?
                    self.df = rdd
                    self.get_data()
                    self.remaining_steps = list(self.step_to_action.keys())

                    super().run()

                    items_to_unpersist = self.spark.sparkContext._jsc.\
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

        kafkaStream.foreachRDD(process_subsets)

        self.ssc.start()
        self.ssc.awaitTermination()
        return self.df


class GetDataKafkaStreaming(Task):
    def __init__(self, config: BaskervilleConfig, steps: list = ()):
        super().__init__(config, steps)
        self.stream_df = None
        self.kafka_params = {
            'kafka.bootstrap.servers': self.config.bootstrap_servers,
            'metadata.broker.list': self.config.kafka.bootstrap_servers,
            'auto.offset.reset': 'largest',
            'group.id': self.config.kafka.consume_group,
            'auto.create.topics.enable': 'true'
        }

    def initialize(self):
        super(GetDataKafkaStreaming, self).initialize()
        self.stream_df = self.spark \
            .readStream \
            .format("kafka") \
            .option(
            "kafka.bootstrap.servers", self.config.kafka.bootstrap_servers
        ).option("subscribe", self.config.kafka.prediction_reply_topic)

    def get_data(self):
        self.stream_df.load()
        self.stream_df.selectExpr(
            "CAST(key AS STRING)", "CAST(value AS STRING)"
        )

    def run(self):
        self.create_runtime()
        self.get_data()
        self.df = self.stream_df.select(
            F.from_json(F.col("value").cast("string"), prediction_schema)
        )
        # todo: match with redis
        self.df = super(GetDataKafkaStreaming, self).run()

        return self.df


class GetDataLog(Task):
    def __init__(self, config, steps=(), group_by_cols=('client_request_host', 'client_ip'),):
        super().__init__(config, steps)
        self.log_paths = self.config.engine.raw_log.paths
        self.group_by_cols = group_by_cols
        self.batch_i = 1
        self.batch_n = len(self.log_paths)
        self.current_log_path = None

    def initialize(self):
        print(f'{self.__class__} GetDataLog initialize...')
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
        ).persist(self.config.spark.storage_level)  # .repartition(*self.group_by_cols)

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
        Splits the data into time bucket length windows and executes all the steps
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
                )) or os.cpu_count()*2,
                column='id',
                lowerBound=bounds.min_id,
                upperBound=bounds.max_id + 1,
                properties=self.conn_properties
            )
        raise NotImplementedError(f'No implementation for "extra_filters"')

    def run(self):
        self.df = self.get_data()
        return super().run()


class Preprocess(MLTask):
    def __init__(
            self,
            config,
            steps=(),
            group_by_cols=('client_request_host', 'client_ip'),
    ):
        super().__init__(config, steps)
        self.data_parser = self.config.engine.data_config.parser
        self.group_by_cols = list(set(group_by_cols))
        self.group_by_aggs = None
        self.post_group_by_aggs = None
        self.columns_to_filter_by = None
        self.drop_if_missing_filter = None
        self.cols_to_drop = None

    def initialize(self):
        print(f'{self.__class__} Preprocess initialize...')
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
        for k, v in self.feature_manager.column_renamings:
            self.df = self.df.withColumnRenamed(k, v)

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
        self.df = self.df.groupBy(
            *self.group_by_cols
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

        if self.model:
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
        # todo: shouldn't this be a renaming?
        self.df = self.df.withColumn('ip', F.col('client_ip'))
        self.df = self.df.withColumn(
            'target', F.col('client_request_host')
        )
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

    def run(self):
        self.handle_missing_columns()
        self.rename_columns()
        self.filter_columns()
        self.handle_missing_values()
        self.normalize_host_names()
        self.add_calc_columns()
        self.group_by()
        self.feature_calculation()
        
        return super().run()


class Predict(MLTask):
    def __init__(self, config: BaskervilleConfig, steps=()):
        super().__init__(config, steps)
        self._can_predict = False
        self._is_initialized = False

    def predict(self):
        self.df = self.model.predict(self.df)

    def run(self):
        self.predict()
        self.df = super(Predict, self).run()
        return self.df


class Train(Task):
    pass


class SaveInStorage(Task):
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
        request_set_columns = RequestSet.columns[:]
        not_common = {
            'prediction', 'model_version', 'label', 'id_attribute',
            'updated_at'
        }.difference(self.df.columns)

        for c in not_common:
            request_set_columns.remove(c)

        # filter the logs df with the request_set columns
        self.df = self.df.select(request_set_columns)

        # save request_sets
        self.logger.debug('Saving request_sets')
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
        self.service_provider.refresh_cache(self.df)
        self.df = super().run()
        return self.df


class PredictionOutput(Task):
    pass


class PredictionInput(Task):
    pass


class Evaluate(Task):
    pass


class ModelUpdate(MLTask):
    pass
