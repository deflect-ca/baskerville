# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import datetime

import itertools
import json
import os
import threading
import traceback

import pyspark
from kafka.errors import TopicAlreadyExistsError
from pyspark.files import SparkFiles

from baskerville.db.dashboard_models import FeedbackContext
from pyspark.sql import functions as F, types as T
from pyspark.sql.types import StringType, StructField, StructType, BooleanType
from pyspark.streaming import StreamingContext
from sqlalchemy.exc import SQLAlchemyError
from user_agents import parse

from baskerville.db import get_jdbc_url
from baskerville.db.models import RequestSet, Model
from baskerville.models.classifier_model import ClassifierModel
from baskerville.models.storage_io import StorageIO
from baskerville.models.incident_detector import IncidentDetector
from baskerville.models.ip_cache import IPCache
from baskerville.models.pipeline_tasks.tasks_base import Task, MLTask, \
    CacheTask
from baskerville.models.config import BaskervilleConfig, TrainingConfig
from baskerville.spark.helpers import map_to_array, load_test, \
    columns_to_dict, get_window, set_unknown_prediction, \
    get_dtype_for_col, \
    handle_missing_col
from baskerville.spark.schemas import features_schema, \
    prediction_schema, get_message_schema, get_data_schema, get_features_schema
from kafka import KafkaProducer
from dateutil.tz import tzutc

# broadcasts
from baskerville.util.banjax_report_consumer import BanjaxReportConsumer
from baskerville.util.elastic_writer import ElasticWriter
from baskerville.util.helpers import parse_config, get_default_data_path
from baskerville.util.helpers import instantiate_from_str, get_model_path
from baskerville.util.kafka_helpers import send_to_kafka, read_from_kafka_from_the_beginning
from baskerville.util.mail_sender import MailSender
from baskerville.util.whitelist_ips import WhitelistIPs
from baskerville.util.whitelist_urls import WhitelistURLs
from pyspark.sql.functions import broadcast, udf

from geoip2 import database
from geoip2.errors import AddressNotFoundError

TOPIC_BC = None
KAFKA_URL_BC = None
CLIENT_MODE_BC = None
OUTPUT_COLS_BC = None
IP_ACC = None


def parse_ua(ua_string):
    # parse library cannot parse None
    if ua_string is None:
        ua_string = ""

    parsed_string = parse(ua_string)

    output = [
        parsed_string.device.brand,
        parsed_string.device.family,
        parsed_string.device.model,

        parsed_string.os.family,
        parsed_string.os.version_string,

        parsed_string.browser.family,
        parsed_string.browser.version_string,

        (parsed_string.is_mobile or parsed_string.is_tablet),
        parsed_string.is_bot
    ]
    # If any of the column have None value it doesn't comply with schema
    # and thus throw Null Pointer Exception
    for i in range(len(output)):
        if output[i] is None:
            output[i] = 'Unknown'
    return output


geoip_schema = StructType([
    StructField('country_name', StringType(), True),
])


@udf(returnType=geoip_schema)
def geoip(ip):
    geo = database.Reader(SparkFiles.get('GeoLite2-Country.mmdb'))

    try:
        result = geo.country(ip)
        pass
    except AddressNotFoundError:
        return {'country': None}

    return {'country': result.names['en']}


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
            'metadata.broker.list': self.config.kafka.connection['bootstrap_servers'],
            'auto.offset.reset': self.config.kafka.auto_offset_reset,
            'group.id': self.config.kafka.consume_group,
            'auto.create.topics.enable': 'true'
        }
        self.consume_topic = self.config.kafka.data_topic

    def create_topic(self, topic):
        from kafka.admin import KafkaAdminClient, NewTopic
        try:
            admin_client = KafkaAdminClient(**self.config.kafka.connection)
            admin_client.create_topics(
                new_topics=[NewTopic(name=topic, num_partitions=1, replication_factor=1)],
            )
        except TopicAlreadyExistsError:
            pass

    def initialize(self):
        super(GetDataKafka, self).initialize()

        # create topic if not exists
        self.create_topic(self.consume_topic)

        self.ssc = StreamingContext(
            self.spark.sparkContext, self.config.engine.time_bucket
        )
        from pyspark.streaming.kafka import KafkaUtils
        self.kafka_stream = KafkaUtils.createDirectStream(
            self.ssc,
            [self.consume_topic],
            kafkaParams=self.kafka_params,
        )

        if self.config.engine.input_is_weblogs:
            self.spark.sparkContext.addFile(os.path.join(get_default_data_path(), 'geoip2', 'GeoLite2-Country.mmdb'))

    def get_data(self):
        if self.config.engine.input_is_weblogs:
            self.df = self.df.map(lambda l: json.loads(l[1]))

            schema = T.StructType([
                T.StructField("message", T.StringType(), True)
            ])
            self.df = self.df.map(lambda x: [x['message']]).toDF(schema=schema)

            regex = '([(\d\.)]+) - \[(.*?)\] "(.*?)" (.*) (.*) (\d+) (\d+) "(.*?)" (.*?) (.*?) (.*?) (.*?) "(.*?)" "(.*?)"'

            self.df = self.df.withColumn('client_ip', F.regexp_extract(F.col('message'), regex, 1))
            self.df = self.df.withColumn('@timestamp', F.regexp_extract(F.col('message'), regex, 2))
            self.df = self.df.withColumn('@timestamp', F.to_timestamp(F.col('@timestamp'), 'dd/MMM/yyyy:HH:mm:ss Z'))

            self.df = self.df.withColumn('request', F.regexp_extract(F.col('message'), regex, 3))
            self.df = self.df.withColumn('client_url', F.regexp_extract(F.col('request'), '(.*) (.*) (.*)', 2))
            self.df = self.df.withColumn('client_request_method',
                                         F.regexp_extract(F.col('request'), '(.*) (.*) (.*)', 1))
            self.df = self.df.drop('request')

            self.df = self.df.withColumn('client_request_host', F.regexp_extract(F.col('message'), regex, 5))
            self.df = self.df.withColumn('http_response_code', F.regexp_extract(F.col('message'), regex, 6))
            self.df = self.df.withColumn('reply_length_bytes', F.regexp_extract(F.col('message'), regex, 7))

            self.df = self.df.withColumn('client_ua', F.regexp_extract(F.col('message'), regex, 8))
            ua_parser_udf = F.udf(lambda z: parse_ua(z), StructType([
                StructField("device_brand", StringType(), False),
                StructField("device_family", StringType(), False),
                StructField("device_model", StringType(), False),

                StructField("os_family", StringType(), False),
                StructField("os_version", StringType(), False),

                StructField("browser_family", StringType(), False),
                StructField("browser_version", StringType(), False),

                StructField("is_mobile", BooleanType(), False),
                StructField("is_bot", BooleanType(), False),
            ]))
            self.df = self.df.withColumn('ua', ua_parser_udf('client_ua'))

            self.df = self.df.withColumn('content_type', F.regexp_extract(F.col('message'), regex, 10))
            self.df = self.df.withColumn('querystring', F.regexp_extract(F.col('message'), regex, 13))

            self.df = self.df.withColumn('geoip', geoip('client_ip'))

            self.df = self.df.drop('message')
        else:
            self.df = self.df.map(lambda l: json.loads(l[1])).toDF(
                self.data_parser.schema
            ).persist(self.spark_conf.storage_level)

        self.df = load_test(
            self.df,
            self.config.engine.load_test,
            self.config.spark.storage_level
        )

    def run(self):
        self.create_runtime()

        def process_subsets(time, rdd):
            self.df_time = time
            self.logger.info(f'Data until {time} from kafka topic \'{self.consume_topic}\'')
            if rdd and not rdd.isEmpty():
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

                    if self.config.engine.log_level == 'DEBUG':
                        items_to_unpersist = self.spark.sparkContext._jsc. \
                            getPersistentRDDs().items()
                        if items_to_unpersist:
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
                self.reset()

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
        self.data_schema = get_data_schema()
        self.message_schema = get_message_schema(self.config.engine.all_features)

    def get_data(self):
        self.df = self.spark.createDataFrame(
            self.df,
            self.data_schema
        ).persist(self.config.spark.storage_level)

        self.df = self.df.withColumn(
            'message',
            F.from_json('message', self.message_schema)
        )

        self.df = self.df.where(F.col('message.id_client').isNotNull()) \
            .withColumn('features', F.col('message.features')) \
            .withColumn('id_client', F.col('message.id_client')) \
            .withColumn('uuid_request_set', F.col('message.uuid_request_set')) \
            .drop('message', 'key').persist(self.config.spark.storage_level)


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
        )  # .persist(
        # self.config.spark.storage_level
        # )
        # self.df.show()
        # json_schema = self.spark.read.json(
        #     self.df.limit(1).rdd.map(lambda row: row.features)
        # ).schema
        # self.df = self.df.withColumn(
        #     'features',
        #     F.from_json('features', json_schema)
        # )


class ReTrain(GetDataKafka):
    def __init__(self, config, steps: list = ()):
        super(ReTrain, self).__init__(config, steps)
        self.in_progress = False
        self.producer = KafkaProducer(**self.config.kafka.connection)

    def run(self):
        current_reply_topic = ''
        id_data = {}
        try:
            # get config
            timestamp, pw_uuid, org_uuid, training_config = self.df.collect()[0]
            current_reply_topic = f'{org_uuid}.{self.config.kafka.data_topic}'
            id_data = dict(pw_uuid=pw_uuid, uuid_organization=org_uuid)
            if not self.in_progress:
                self.in_progress = True
                # todo: validate org_uuid
                training_config = TrainingConfig(
                    parse_config(data=training_config)
                ).validate()
                if not training_config.errors:
                    reply = {
                        **id_data,
                        'message': 'Received configuration and will start training.',
                        'success': True,
                        'pending': True
                    }
                    # set the training config on all steps:
                    self.config.engine.training = training_config
                    self.set_on_all_steps('config', self.config)
                    self.producer.send(
                        current_reply_topic,
                        bytes(json.dumps(reply).encode('utf-8'))
                    )
                self.df = super().run()
                finished_reply = {
                    **id_data,
                    'message': 'Finished training.',
                    'success': True,
                    'pending': False
                }
                self.producer.send(
                    current_reply_topic,
                    bytes(json.dumps(finished_reply).encode('utf-8'))
                )
            else:
                abort = {
                    **id_data,
                    'message': 'Cannot start another training job.',
                    'success': False,
                    'pending': False
                }
                self.producer.send(
                    current_reply_topic,
                    bytes(json.dumps(abort).encode('utf-8'))
                )
        except Exception:
            self.in_progress = False
            traceback.print_exc()
            if current_reply_topic and id_data:
                error_reply = {
                    **id_data,
                    'message': 'Failed to retrain, please, '
                               'check the logs and try again',
                    'success': False,
                    'pending': False
                }
                print(error_reply)
                self.producer.send(
                    current_reply_topic,
                    bytes(json.dumps(error_reply).encode('utf-8'))
                )
        return self.df


class GetDataKafkaStreaming(Task):
    def __init__(self, config: BaskervilleConfig, steps: list = ()):
        super().__init__(config, steps)
        self.stream_df = None
        self.kafka_params = {
            'kafka.bootstrap.servers': self.config.connection['bootstrap_servers'],
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
            .option("kafka.bootstrap.servers", self.config.kafka.connection['bootstrap_servers']) \
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
        self.service_provider.create_runtime()
        self.runtime.file_name = self.current_log_path
        self.runtime.comment = f'batch runtime {self.batch_i} of {self.batch_n}'
        self.db_tools.session.commit()
        self.logger.info('Created runtime {}'.format(self.runtime.id))

    def get_data(self):
        """
        Gets the dataframe according to the configuration
        :return: None
        """

        self.df = self.spark.read.json(
            self.current_log_path
        )  # .persist(self.config.spark.storage_level)

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
        if len(self.df.head(1)) == 0:
            self.logger.info('No data in to process.')
        else:
            for window_df in get_window(
                    self.df, self.time_bucket, self.config.spark.storage_level, self.logger
            ):
                self.df = window_df.repartition(
                    *self.group_by_cols
                )  # .persist(self.config.spark.storage_level)
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


class GetDataFromStorage(Task):
    def __init__(
            self,
            config: BaskervilleConfig,
            from_date=None,
            to_date=None,
            load_one_random_batch_from_every_hour=True,
            steps: list = ()
    ):
        super().__init__(config, steps)
        self.storage_io = None
        self.from_date = from_date
        self.to_date = to_date
        self.load_one_random_batch_from_every_hour = load_one_random_batch_from_every_hour

    def initialize(self):
        self.storage_io = StorageIO(
            storage_path=self.config.engine.storage_path,
            spark=self.spark,
            logger=self.logger
        )

    def run(self):
        self.df = self.storage_io.load(self.from_date, self.to_date,
                                       load_one_random_batch_from_every_hour=self.load_one_random_batch_from_every_hour)
        self.df = super().run()
        return self.df


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
            training_days=None,
            sampling_percentage=10.0
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
        self.sampling_percentage = sampling_percentage

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
        if bounds.rows == 0:
            self.logger.info('No data for this period')
            raise ValueError('No data for this period, check configuration.')

        q = f'(select id, {",".join(self.columns_to_keep)} ' \
            f'from request_sets where id >= {bounds.min_id}  ' \
            f'and id <= {bounds.max_id} and stop >= \'{from_date}\' ' \
            f'and stop <=\'{to_date}\') as request_sets'

        # q = f'(select id, {",".join(self.columns_to_keep)} ' \
        #     f'from request_sets where id >= {bounds.min_id}  ' \
        #     f'and id <= {bounds.max_id} and stop >= \'{from_date}\' ' \
        #     f'and stop <=\'{to_date}\' ' \
        #     f'TABLESAMPLE SYSTEM ({self.sampling_percentage}) ) as request_sets '

        return self.spark.read.jdbc(
            url=self.db_url,
            table=q,
            numPartitions=int(self.spark.conf.get(
                'spark.sql.shuffle.partitions'
            ), os.cpu_count() * 2),
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
        self.whitelist_urls = WhitelistURLs(
            url=config.engine.url_whitelist_urls,
            logger=self.logger,
            refresh_period_in_seconds=config.engine.dashboard_config_refresh_period_in_seconds
        )
        self.whitelist_ips = WhitelistIPs(
            url=config.engine.url_origin_ips,
            url2=config.engine.url_whitelist_ips,
            logger=self.logger,
            refresh_period_in_seconds=config.engine.dashboard_config_refresh_period_in_seconds
        )

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

    def handle_missing_values(self):
        self.df = self.data_parser.fill_missing_values(self.df)

    def white_list_ips(self):
        whitelist = []
        if self.config.engine.white_list_ips:
            whitelist = self.config.engine.white_list_ips
        if self.whitelist_ips.get_global_ips():
            whitelist += self.whitelist_ips.get_global_ips()
        if len(whitelist) > 0:
            df_ips = self.spark.createDataFrame(
                [[ip] for ip in whitelist],
                schema=T.StructType([T.StructField("client_ip", T.StringType())])
            )
            self.df = self.df.join(
                broadcast(df_ips),
                on=['client_ip'], how='left_anti'
            )

        host_ips = self.whitelist_ips.get_host_ips()

        if len(host_ips.keys()) == 0:
            self.logger.warning('whitelist_ips.get_host_ips() empty')
            return

        host_ip_list = []
        for host, v in self.whitelist_ips.get_host_ips().items():
            for ip in v:
                host_ip_list.append([host, ip])

        df_host_ips = self.spark.createDataFrame(
            host_ip_list,
            schema=T.StructType([T.StructField('target_original', T.StringType()),
                                 T.StructField("client_ip", T.StringType())])
        )

        self.df = self.df.join(
            broadcast(df_host_ips),
            on=['target_original', 'client_ip'], how='left_anti'
        )

    def white_list_urls(self):
        urls = []
        if self.config.engine.white_list_urls:
            urls = self.config.engine.white_list_urls
        urls_from_dashboard = self.whitelist_urls.get()
        if urls_from_dashboard:
            urls += urls_from_dashboard
        urls = list(set(urls))

        domains = []
        prefixes = []
        matches = []
        stars = []
        for url in urls:
            if url.find('/') < 0:
                domains.append(url)
            else:
                star_pos = url.find('*')
                if url.find('*') < 0:
                    matches.append(url)
                else:
                    if star_pos == len(url) - 1:
                        prefixes.append(url[:-1])
                    else:
                        stars.append((url[:star_pos], url[star_pos + 1:]))

        # filter out only the exact domain match
        if len(domains) > 0:
            for domain in domains:
                self.df = self.df.filter(~F.col('target_original').contains(domain))

        # concatenate the full path URL
        self.df = self.df.withColumn('url', F.concat(F.col('target_original'), F.col('client_url')))

        # filter out the url match
        if len(matches) > 0:
            self.df = self.df.filter(~F.col('url').isin(matches))

        # filter out the url prefix
        @F.udf(returnType=T.BooleanType())
        def filter_prefixes(url):
            for url_prefix in prefixes:
                if url and url.startswith(url_prefix):
                    return False
            return True

        self.df = self.df.filter(filter_prefixes('url'))

        # filter out the stars
        @F.udf(returnType=T.BooleanType())
        def filter_stars(url):
            for star in stars:
                if url and url.startswith(star[0]) and url.endswith(star[1]):
                    return False
            return True

        self.df = self.df.filter(filter_stars('url'))

    def normalize_host_names(self):
        """
        From www.somedomain.tld keep somedomain
        # todo: improve this and remove udf
        # todo: keep original target in a separate field in db
        :return:
        """
        from baskerville.spark.udfs import udf_normalize_host_name
        from baskerville.spark.udfs import udf_remove_www
        self.df = self.df.fillna({'client_request_host': ''})
        self.df = self.df.withColumn('target_original',
                                     udf_remove_www(F.col('client_request_host').cast(T.StringType())))
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
        # self.df.persist(self.config.spark.storage_level)

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
            'uuid_request_set', F.monotonically_increasing_id()
        ).withColumn(
            'uuid_request_set',
            F.concat_ws(
                '_',
                F.col('id_client'),
                F.col('uuid_request_set'),
                F.col('stop').cast('long').cast('string'))
        )
        # todo: monotonically_increasing_id guarantees uniqueness within
        #  the current batch, this will cause conflicts with caching - use
        # e.g. the timestamp too to avoid this

    def rename_timestamp_column(self):
        if self.config.engine.input_timestamp_column != '@timestamp':
            self.df = self.df.withColumn('@timestamp', F.col(self.config.engine.input_timestamp_column))

    def run(self):
        self.handle_missing_columns()
        self.rename_timestamp_column()
        self.normalize_host_names()
        # self.df = self.df.repartition('target_original')
        self.white_list_ips()
        self.white_list_urls()
        self.df = self.df.repartition(*self.group_by_cols).persist(self.spark_conf.storage_level)
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
        self._is_initialized = False
        self.classifier_model = None

    def initialize(self):
        super().initialize()

        if self.config.engine.classifier_model_path:
            self.classifier_model = ClassifierModel()
            self.classifier_model.load(self.config.engine.classifier_model_path, spark_session=self.spark)

    def handle_missing_features(self):
        """
        Add any missing features and fill any missing values with each
        feature's default value
        """
        from baskerville.features import FEATURE_NAME_TO_CLASS

        for f_ in self.model.features:
            feat_dict_col = f'features.{f_}'
            default_value = FEATURE_NAME_TO_CLASS[f_].feature_default
            self.df = handle_missing_col(
                self.df,
                feat_dict_col,
                default_value
            )
            self.df = self.df.fillna(default_value, subset=[feat_dict_col])

    def predict(self):
        if self.classifier_model:
            self.logger.info('Classifier predicting...')
            self.df = self.classifier_model.predict(self.df)
        else:
            self.logger.info('Classifier model is not used')

        if self.model:
            self.df = self.model.predict(self.df)
        else:
            self.df = set_unknown_prediction(self.df)
            self.logger.error('No model to predict')

    def run(self):
        if self.df and self.df.head(1):
            self.df = self.df.persist(self.config.spark.storage_level)
            if self.config.engine.handle_missing_features:
                self.handle_missing_features()
            self.predict()
            self.df = super(Predict, self).run()
            self.reset()
        return self.df


class RefreshModel(MLTask):
    """
    Check for a new model and load a new model in ServiceProvider.
    """

    def __init__(self, config: BaskervilleConfig, steps=()):
        super().__init__(config, steps)

    def run(self):
        self.service_provider.refresh_model()
        return self.df


class SaveDfInPostgres(Task):
    def __init__(
            self,
            config,
            steps=(),
            table_model=RequestSet,
            json_cols=('features',),
            mode='append'
    ):
        super().__init__(config, steps)
        self.table_model = table_model
        self.json_cols = json_cols
        self.mode = mode

    def run(self):
        self.config.database.conn_str = self.db_url
        self.df.na.drop()
        # save_df_to_table(
        #     self.df,
        #     self.table_model.__tablename__,
        #     self.config.database.__dict__,
        #     json_cols=self.json_cols,
        #     storage_level=self.config.spark.storage_level,
        #     mode=self.mode,
        #     db_driver=self.config.spark.db_driver
        # )
        self.df = super().run()
        return self.df


class Save(SaveDfInPostgres):
    """
    Saves dataframe in Postgres (current backend)
    """

    def __init__(self, config,
                 steps=(),
                 table_model=RequestSet,
                 json_cols=('features',),
                 mode='append',
                 not_common=(
                     'prediction',
                     'prediction_anomaly',
                     'prediction_classifier',
                     'model_version',
                     'label',
                     'id_attribute',
                     'updated_at')
                 ):
        self.not_common = set(not_common)
        super().__init__(config, steps, table_model, json_cols, mode)

    def prepare_to_save(self):
        table_columns = self.table_model.columns[:]
        not_common = self.not_common.difference(self.df.columns)

        for c in not_common:
            table_columns.remove(c)

        if len(self.df.columns) < len(table_columns):
            # log and let it blow up; we need to know that we cannot save
            self.logger.error(
                'The input df columns are different than '
                'the actual table columns'
            )

        self.df = self.df.select(table_columns)
        self.df = self.df.withColumn(
            'created_at',
            F.current_timestamp()
        )
        # self.df.show(1, False)

    def run(self):
        Save.prepare_to_save(self)
        # save request_sets
        self.logger.debug('Saving request_sets')
        self.df = super().run()
        return self.df


class SaveToStorage(Task):
    def __init__(
            self,
            config,
            steps=()
    ):
        super().__init__(config, steps)
        self.storage_io = None

    def initialize(self):
        self.storage_io = StorageIO(self.config.engine.storage_path, spark=self.spark, logger=self.logger)

    def run(self):
        if not self.config.engine.save_to_storage:
            return self.df

        self.storage_io.save(self.df, self.df_time)
        return self.df


class SaveFeedback(SaveDfInPostgres):
    def __init__(self, config,
                 steps=(),
                 table_model=RequestSet,
                 json_cols=('features',),
                 mode='append',
                 not_common=(
                     'prediction',
                     'model_version',
                     'label',
                     'id_attribute',
                     'updated_at')
                 ):
        self.not_common = set(not_common)
        super().__init__(config, steps, table_model, json_cols, mode)

    def upsert_feedback_context(self):
        new_ = False
        success = False
        try:
            self.df = self.df.withColumn('id_fc', F.lit(None))
            uuid_organization, feedback_context = self.df.select(
                'uuid_organization', 'feedback_context'
            ).collect()[0]
            if feedback_context:
                fc = self.db_tools.session.query(FeedbackContext).filter_by(
                    uuid_organization=uuid_organization
                ).filter_by(
                    start=feedback_context.start
                ).filter_by(stop=feedback_context.stop).first()
                if not fc:
                    fc = FeedbackContext()
                    new_ = True
                # fc.uuid_org = feedback_context.uuid_org
                fc.reason = feedback_context.reason
                fc.reason_descr = feedback_context.reason_descr
                fc.start = feedback_context.start
                fc.stop = feedback_context.stop
                fc.ip_count = feedback_context.ip_count
                fc.notes = feedback_context.notes
                fc.progress_report = feedback_context.progress_report
                if new_:
                    self.db_tools.session.add(fc)
                self.db_tools.session.commit()
                self.df = self.df.withColumn('id_fc', F.lit(fc.id))
                success = True
            else:
                self.logger.info('No feedback context.')
                success = True
        except SQLAlchemyError as sqle:
            traceback.print_exc()
            self.db_tools.session.rollback()
            success = False
            self.logger.error(str(sqle))
            # todo: what should the handling be?
        except Exception as e:
            traceback.print_exc()
            success = False
            self.logger.error(str(e))
            # todo: what should the handling be?
        return success

    def prepare_to_save(self):
        try:
            success = self.upsert_feedback_context()
            success = True
            if success:
                # explode submitted feedback first
                # updated feedback will be inserted and identical uuid_request_set
                # can be filtered out with created_at or max(id)
                self.df = self.df.select(
                    'uuid_organization',
                    'id_context',
                    F.col('id_fc').alias('sumbitted_context_id'),
                    F.explode('feedback').alias('feedback')
                ).cache()
                self.df = self.df.select(
                    F.col('uuid_organization').alias('top_uuid_organization'),
                    F.col('id_context').alias('client_id_context'),
                    F.col('sumbitted_context_id'),
                    *[F.col(f'feedback.{c}').alias(c) for c in self.table_model.columns]
                ).cache()
                self.df = self.df.withColumnRenamed('id_context', 'client_id_context')
                self.df = self.df.drop('updated_at')
                self.df = self.df.withColumn('id_context', F.col('sumbitted_context_id')).drop('sumbitted_context_id')
                self.df.show()
                Save.prepare_to_save(self)
                self.df = SaveDfInPostgres.run(self)
            self.df = self.df.groupBy('uuid_organization', 'id_context').count().toDF()
            self.df = self.df.withColumn('success', F.lit(True))
        except Exception as exp:
            self.logger.error(exp)
            self.df = self.df.withColumn('success', F.lit(False))

    def run(self):
        self.upsert_feedback_context()
        self.prepare_to_save()
        return self.df


class RefreshCache(CacheTask):
    def run(self):
        self.service_provider.refresh_cache(self.df)
        self.df.unpersist()
        del self.df
        self.df = None
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
        df_sensitive = self.df.withColumn("features", F.to_json("features"))

        df_sensitive = df_sensitive.withColumn(
            'start', F.date_format(F.col('start'), 'yyyy-MM-dd HH:mm:ss')
        ).withColumn(
            'stop', F.date_format(F.col('stop'), 'yyyy-MM-dd HH:mm:ss')
        ).withColumn(
            'first_ever_request', F.date_format(F.col('stop'), 'yyyy-MM-dd HH:mm:ss')
        )

        if self.config.engine.use_kafka_for_sensitive_data:
            self.logger.info('Sending sensitive data to kafka...')
            send_to_kafka(
                self.spark,
                df=df_sensitive,
                columns=df_sensitive.columns,
                topic=self.config.engine.kafka_topic_sensitive,
                use_partitions=self.config.engine.kafka_send_by_partition
            )
            self.logger.info('Done.')
        else:
            df_sensitive.write.format(
                'org.apache.spark.sql.redis'
            ).mode(
                'append'
            ).option(
                'table', self.table_name
            ).option(
                'ttl', self.ttl
            ).option(
                'key.column', 'uuid_request_set'
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
        self.df_sensitive = None
        self.table_name = table_name

    def get_sensitive_schema(self):
        features = T.StructType()
        for feature in self.config.engine.all_features.keys():
            features.add(T.StructField(
                name=feature,
                dataType=T.StringType(),
                nullable=True))

        return T.StructType([
            T.StructField("target", T.StringType(), True),
            T.StructField('ip', T.StringType(), True),
            T.StructField('num_requests', T.IntegerType(), True),
            T.StructField('target_original', T.StringType(), True),
            T.StructField('first_ever_request', T.StringType(), True),
            T.StructField('id_runtime', T.IntegerType(), True),
            T.StructField('time_bucket', T.IntegerType(), True),
            T.StructField('start', T.StringType(), True),
            T.StructField('stop', T.StringType(), True),
            T.StructField('subset_count', T.IntegerType(), True),
            T.StructField('dt', T.FloatType(), True),
            T.StructField("features", features),
            T.StructField('total_seconds', T.FloatType(), True),
            T.StructField('id_client', T.StringType(), True),
            T.StructField('id_request_sets', T.StringType(), True)
        ])

    def run(self):
        if self.config.engine.use_kafka_for_sensitive_data:
            self.logger.info('Reading sensitive data from kafka...')
            self.df_sensitive = read_from_kafka_from_the_beginning(
                bootstrap_servers=self.config.kafka.connection['bootstrap_servers'],
                topic=self.config.engine.kafka_topic_sensitive,
                schema=self.get_sensitive_schema(),
                spark=self.spark
            ).alias('df_sensitive')
            self.logger.info('Done.')
        else:
            self.df_sensitive = self.spark.read.format(
                'org.apache.spark.sql.redis'
            ).option(
                'table', self.table_name
            ).option(
                'key.column', 'uuid_request_set'
            ).load().alias('df_sensitive')

        count = self.df.count()
        self.df_sensitive = self.df_sensitive.withColumn('start', F.to_timestamp(F.col('start'), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn('stop', F.to_timestamp(F.col('stop'), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn('first_ever_request', F.to_timestamp(F.col('first_ever_request'), "yyyy-MM-dd HH:mm:ss"))

        self.df = self.df.alias('df')
        self.df = self.df.join(
            self.df_sensitive, on=['id_client', 'uuid_request_set'], how='inner'
        ).drop('df.id_client', 'df.uuid_request_set')

        if self.df:
            merge_count = self.df.count()

            if (merge_count > 0) and (count != merge_count):
                self.logger.warning('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                self.logger.warning('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                self.logger.warning('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                self.logger.warning('Join sensitive data issue. Probably postprocessing is underperforming.')
                self.logger.warning(f'Batch count = {count}. After merge count = {merge_count}')
                self.logger.warning('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                self.logger.warning('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
                self.logger.warning('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
        else:
            self.logger.warning('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
            self.logger.warning(
                f'No df after merging with redis: df.count={count}, df_sensitive.count={self.df_sensitive.count()}'
            )
            self.logger.warning('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')

        self.df = super().run()
        return self.df


class SendToKafka(Task):
    def __init__(
            self,
            config: BaskervilleConfig,
            columns,
            topic=None,
            cc_to_client=False,
            client_topic=None,
            send_to_clearing_house=False,
            steps: list = (),
    ):
        super().__init__(config, steps)
        self.columns = columns
        self.topic = topic
        self.cc_to_client = cc_to_client
        self.client_topic = client_topic

        if send_to_clearing_house:
            self.connection = self.config.kafka.clearing_house_connection
        else:
            self.connection = self.config.kafka.connection

        self.client_connections = self.config.kafka.client_connections

    def run(self):
        self.logger.info(f'Sending to kafka topic \'{self.topic}\'...')

        send_to_kafka(
            spark=self.spark,
            df=self.df,
            columns=self.columns,
            topic=self.topic,
            connection=self.connection,
            cc_to_client=self.cc_to_client,
            client_topic=self.client_topic,
            client_connections=self.client_connections,
            use_partitions=self.config.engine.kafka_send_by_partition,
            logger=self.logger
        )

        return self.df


class Train(Task):

    def __init__(
            self,
            config: BaskervilleConfig,
            steps: list = (),
            convert_features_from_json=False
    ):
        super().__init__(config, steps)
        self.model = None
        self.training_conf = self.config.engine.training
        self.engine_conf = self.config.engine
        self.convert_features_from_json = convert_features_from_json

    def initialize(self):
        super().initialize()

    def load_dataset(self, df, features):
        dataset = df  # .persist(self.spark_conf.storage_level)

        max_samples = self.training_conf.data_parameters.get('max_samples_per_host')
        if max_samples:
            self.logger.debug(f'Sampling with max_samples_per_host='
                              f'{max_samples}...')
            counts = dataset.groupby('target').count()
            counts = counts.withColumn('fraction', max_samples / F.col('count'))
            fractions = dict(counts.select('target', 'fraction').collect())
            for key, value in fractions.items():
                if value > 1.0:
                    fractions[key] = 1.0
            dataset = dataset.sampleBy('target', fractions, 777)

        self.logger.debug('Unwrapping features from json...')
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
            # fixme: bug: .alias(column) will give feature.feature_name
            dataset = dataset.withColumn(column, F.col(column).cast(feature_class.spark_type()).alias(column))

        self.logger.debug(f'Loaded {dataset.count()} rows dataset.')
        return dataset

    def save(self):
        """
        Save the models on disc and add a baskerville.db.Model in the database
        :return: None
        """
        model_path = get_model_path(self.engine_conf.storage_path, self.model.__class__.__name__)
        self.logger.debug(f'Saving new model to: {model_path}')
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

        dataset = self.load_dataset(self.df, self.model.features) if self.convert_features_from_json else self.df

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
            'uuid_request_set',
            'prediction',
            'score',
            'stop',
            *self.feature_manager.active_feature_names
        ).write.format('io.tiledb.spark').option(
            'uri', f'{get_default_data_path()}/tiledbstorage'
        ).option(
            'schema.dim.0.name', 'uuid_request_set'
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
            'rowkey': 'uuid_request_set',
            'columns': {
                'uuid_request_set': {'cf': 'rowkey', 'col': 'uuid_request_set', 'type': 'string'},
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
            'uuid_request_set',
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
            'uuid_request_set',
            'prediction',
            'score',
            'stop',
            *self.feature_manager.active_feature_names
        ).write.format('io.tiledb.spark').option(
            'uri', f'{get_default_data_path()}/tiledbstorage'
        ).option(
            'schema.dim.0.name', 'uuid_request_set'
        ).save()

    def run(self):
        self.logger.debug('Saving features...')
        self.save()
        self.df = super().run()
        return self.df


def dynamic_threshold(score, attack_prediction, threshold, threshold_attack):
    if not score:
        return 0

    if attack_prediction == 1:
        if score > threshold_attack:
            return 1
        else:
            return 0
    else:
        if score > threshold:
            return 1
        else:
            return 0


def prediction(prediction_anomaly, prediction_classifier, attack_prediction):
    if attack_prediction == 1:
        if (prediction_anomaly == 1) or (prediction_classifier == 1):
            return 1
        else:
            return 0
    else:
        if (prediction_anomaly == 1) and (prediction_classifier == 1):
            return 1
        else:
            return 0


class AttackDetection(Task):
    """
    Calculates prediction per IP, attack_score per Target, regular vs anomaly counts, attack_prediction
    """

    def __init__(self, config, steps=()):
        super().__init__(config, steps)
        self.report_consumer = BanjaxReportConsumer(config, self.logger)
        self.low_rate_attack_schema = None
        self.time_filter = None
        self.lra_condition = None
        self.features_schema = get_features_schema(self.config.engine.all_features)
        if config.incident_detector is not None:
            self.incident_detector = IncidentDetector(
                db_config=config.database,
                mail_sender=MailSender(**config.mail) if config.mail else None,
                logger=self.logger,
                **config.incident_detector
            )
        else:
            self.incident_detector = None

    def initialize(self):
        lr_attack_period = self.config.engine.low_rate_attack_period
        lra_total_req = self.config.engine.low_rate_attack_total_request
        # initialize these here to make sure spark session has been initialized
        self.low_rate_attack_schema = T.StructType([T.StructField(
            name='request_total', dataType=StringType(), nullable=True
        )])
        self.time_filter = (
            F.abs(F.unix_timestamp(F.col('stop'))) - F.abs(F.unix_timestamp(F.col('start')))
        )
        self.lra_condition = (
            ((F.col('features.request_total') > lr_attack_period[0]) &
             (self.time_filter > lra_total_req[0])) |
            ((F.col('features.request_total') > lr_attack_period[1]) &
             (self.time_filter > lra_total_req[1]))
        )

        if self.incident_detector is not None:
            self.incident_detector.start()

        consumer_thread = threading.Thread(target=self.report_consumer.run)
        consumer_thread.start()

    def classify_anomalies(self):
        self.logger.info('Anomaly thresholding...')
        if self.incident_detector:
            self.logger.info('Getting hosts with incidents...')
            hosts = self.incident_detector.get_hosts_with_incidents()
            self.logger.info(f'Number of hosts under attack {len(hosts)}.')

            self.df = self.df.withColumn('attack_prediction',
                                         F.when(F.col('target').isin(hosts),
                                                F.lit(1)).otherwise(F.lit(0)))
        else:
            hosts = []
            self.df = self.df.withColumn('attack_prediction', F.lit(1))

        self.logger.info('Dynamic thresholds calculation...')
        self.df = self.df.withColumn('prediction_anomaly', F.udf(dynamic_threshold, T.IntegerType())(
            'score', 'attack_prediction',
            F.lit(self.config.engine.anomaly_threshold),
            F.lit(self.config.engine.anomaly_threshold_during_incident)
        ))

        self.df = self.df.withColumn('prediction_classifier', F.udf(dynamic_threshold, T.IntegerType())(
            'classifier_score', 'attack_prediction',
            F.lit(self.config.engine.classifier_threshold),
            F.lit(self.config.engine.classifier_threshold_during_incident)
        ))

        self.df = self.df.withColumn('prediction', F.udf(prediction, T.IntegerType())(
            'prediction_anomaly', 'prediction_classifier', 'attack_prediction'
        ))

    def detect_low_rate_attack(self):
        if not self.config.engine.low_rate_attack_enabled:
            self.logger.info('Skipping low rate attack detection.')
            self.df = self.df.withColumn('low_rate_attack', F.lit(0))
            return

        self.logger.info('Low rate attack detecting...')
        if get_dtype_for_col(self.df, 'features') == 'string':
            self.df = self.df.withColumn(
                'features',
                F.from_json('features', self.low_rate_attack_schema)
            )
        self.df = self.df.withColumn(
            'features.request_total',
            F.col('features.request_total').cast(
                T.DoubleType()
            ).alias('features.request_total')
        )
        self.df = self.df.withColumn(
            'low_rate_attack',
            F.when(self.lra_condition, F.lit(1)).otherwise(F.lit(0))
        )

    def run(self):
        if get_dtype_for_col(self.df, 'features') == 'string':
            self.logger.info('Unwrapping features from json...')

            # this can be true when running the raw log pipeline
            self.df = self.df.withColumn(
                "features",
                F.from_json("features", self.features_schema)
            )

        self.classify_anomalies()
        self.detect_low_rate_attack()

        self.df = super().run()
        return self.df


class Challenge(Task):
    def __init__(
            self,
            config: BaskervilleConfig, steps=(),
            attack_cols=('prediction', 'low_rate_attack')
    ):
        super().__init__(config, steps)
        self.attack_cols = attack_cols
        self.white_list_ips = set(self.config.engine.white_list_ips)
        self.attack_filter = None
        self.producer = None
        self.ip_cache = IPCache(config, self.logger)
        if config.elastic:
            self.elastic_writer = ElasticWriter(host=config.elastic.host,
                                                port=config.elastic.port,
                                                user=config.elastic.user,
                                                password=config.elastic.password)
        else:
            self.elastic_writer = None

    def initialize(self):
        # global IP_ACC
        # from baskerville.spark.helpers import DictAccumulatorParam
        # IP_ACC = self.spark.sparkContext.accumulator(defaultdict(int),
        #                                              DictAccumulatorParam(
        #                                                  defaultdict(int)))
        self.attack_filter = self.get_attack_filter()
        self.producer = KafkaProducer(
            bootstrap_servers=self.config.kafka.connection['bootstrap_servers'])

        def send_to_kafka(
                kafka_servers, topic, rows, cmd_name='challenge_host',
                id_client=None
        ):
            """
            Creates a kafka producer and sends the rows one by one,
            along with the specified command (challenge_[host, ip])
            :returns: False if something went wrong, true otherwise
            """
            # global IP_ACC
            try:
                from kafka import KafkaProducer
                producer = KafkaProducer(
                    bootstrap_servers=kafka_servers
                )
                for row in rows:
                    from baskerville.spark.udfs import get_msg
                    message = get_msg(row, cmd_name)
                    producer.send(topic, get_msg(row, cmd_name))
                    if id_client:
                        producer.send(f'{topic}.{id_client}', message)
                    # if cmd_name == 'challenge_ip':
                    #     IP_ACC += {row: 1}
                producer.flush()
            except Exception:
                import traceback
                traceback.print_exc()
                return False
            return True

    def get_attack_filter(self):
        filter_ = None
        for f_ in [(F.col(a) == 1) for a in self.attack_cols]:
            if filter_ is None:
                filter_ = f_
            else:
                filter_ = filter_ | f_
        return filter_

    def send_challenge(self):
        df_ips = self.get_attack_df()
        if self.config.engine.challenge == 'ip':
            self.df = self.df.withColumn('challenged', F.lit(0))

            # host white listing
            hosts = []
            if self.config.engine.white_list_hosts:
                hosts = self.config.engine.white_list_hosts

            if len(hosts):
                df_white_list_hosts = self.spark.createDataFrame(
                    [[host] for host in set(hosts)], ['target']).withColumn('white_list_host', F.lit(1))
                df_ips = df_ips.join(
                    df_white_list_hosts, on='target', how='left'
                ).persist()
                df_ips = df_ips.where(F.col('white_list_host').isNull())

            ips = [(r['ip'], r['target'], r['low_rate_attack']) for r in df_ips.collect()]
            ips = self.ip_cache.update(ips)
            num_records = len(ips)
            if num_records > 0:
                # challenged_ips = self.spark.createDataFrame(
                #     [[ip, 1] for ip in ips], ['ip', 'challenged']
                # )
                self.df = self.df.withColumn(
                    'challenged',
                    F.when(F.col('ip').isin([f'{ip}' for ip, _, _ in ips]), 1).otherwise(0)
                )
                # self.df = self.df.join(challenged_ips, on='ip', how='left')
                # self.df = self.df.fillna({'challenged': 0})

                self.logger.info(
                    f'Sending {num_records} IP challenge commands to '
                    f'kafka topic \'{self.config.kafka.banjax_command_topic}\'...')
                null_ips = False
                for ip, target, _ in ips:
                    if ip:
                        message = json.dumps(
                            {
                                'Name': 'challenge_ip',
                                'Value': ip,
                                'host': target,
                                'source': 'bask'
                            }
                        ).encode('utf-8')
                        self.producer.send(self.config.kafka.banjax_command_topic, message,
                                           key=bytearray(target, encoding='utf8'))
                    else:
                        null_ips = True

                if self.elastic_writer:
                    with self.elastic_writer as elastic_writer:
                        for ip, target, low_rate_attack in ips:
                            if ip:
                                elastic_writer.write_challenge(ip, host=target,
                                                               reason='low_rate' if low_rate_attack else 'anomaly')

                if null_ips:
                    self.logger.info('Null ips')
                    self.logger.info(f'{ips}')

                self.producer.flush()
        #
        # return

        # # global IP_ACC
        # self.df = self.df.withColumn('challenged', F.lit(0))
        # if self.config.engine.challenge:
        #     df_to_challenge = None
        #     col_of_interest = None
        #     cmd = f'challenge_{self.config.engine.challenge}'
        #
        #     if self.config.engine.challenge == 'host':
        #         col_of_interest = 'target_original'
        #         df_to_challenge = self.df.select(
        #             'ip', 'target', 'target_original'
        #         ).where(
        #             F.col('attack_prediction') == 1
        #         )
        #         # df_to_challenge = df_to_challenge.select('target').distinct().join(
        #         #     self.df.select('target', 'target_original', 'ip'),
        #         #     on='target', how='left'
        #         # )
        #
        #     elif self.config.engine.challenge == 'ip':
        #         col_of_interest = 'ip'
        #         df_to_challenge = self.df.select('ip', 'target').
        #         where( # this does not look right. Why (F.col('attack_prediction') == 1) & (F.col('prediction') == 1)?
        #             (F.col('attack_prediction') == 1) &
        #             (F.col('prediction') == 1) |
        #             (F.col('low_rate_attack') == 1)
        #         )
        #     else:
        #         self.logger.info(
        #             f'Not implemented challenging method: '
        #             f'{self.config.engine.challenge}'
        #         )
        #
        #     if df_to_challenge and df_to_challenge.head(1) and col_of_interest:
        #         # apply whitelist for both ips and targets
        #         df_to_challenge = self.apply_whitelist(df_to_challenge)
        #         print(df_to_challenge.head(1))
        #         if df_to_challenge.head(1):
        #             df_to_challenge = send_to_kafka_by_partition_id(
        #                 df_to_challenge.select(F.col(col_of_interest).alias('rows')).where(F.col('to_challenge')==True).distinct(),
        #                 self.config.kafka.bootstrap_servers,
        #                 self.config.kafka.banjax_command_topic,
        #                 cmd,
        #                 id_client=None,
        #                 udf_=self.udf_send_to_kafka
        #             )
        #             df_to_challenge = df_to_challenge.withColumnRenamed(
        #                 'sent_to_kafka',
        #                 'challenged'
        #             )
        #             print('df_to_challenge:')
        #             print(df_to_challenge.head(1))
        #
        #             if self.config.engine.challenge == 'ip':
        #                 # todo: host
        #                 collected_ips = df_to_challenge.select('rows').collect()
        #                 print(collected_ips)
        #                 for r in collected_ips:
        #                     self.ip_cache.update(r.rows)
        #                 # self.ip_cache.update(list(IP_ACC.value.keys()))
        #                 # # reset accumulator
        #                 # IP_ACC.value = defaultdict(int)
        #                 self.df = self.df.join(
        #                     df_to_challenge.select(
        #                         F.explode(F.col('rows')).alias('ip'),
        #                         F.col('challenged').alias('rchallenged')
        #                     ).where(F.col('challenged')==True),
        #                     on='ip', how='left'
        #                 ).withColumn(
        #                     'challenged',
        #                     F.when(
        #                         F.col('rchallenged').isNotNull(), 1
        #                     ).otherwise(0)
        #                 ).drop('rchallenged')
        # else:
        #     self.logger.debug('No challenge flag is set, moving on...')

    def get_attack_df(self):
        return self.df.select('ip', 'target', 'low_rate_attack').where(
            # self.attack_filter
            (F.col('prediction') == 1) | (F.col('low_rate_attack') == 1)
        )

    def filter_out_load_test(self):
        if self.config.engine.load_test:
            self.df = self.df.select(
                "*"
            ).where(
                ~F.col('ip').contains('_load_test')
            ).persist(self.config.spark.storage_level)
            self.logger.debug(
                'Filtering out the load test duplications before challenging..'
            )
            self.df.select(
                F.col('target').contains('_load_test')
            ).show()

    def run(self):
        # if df_has_rows(self.df):
        self.df = self.df.withColumn('challenged', F.lit(0))
        self.filter_out_load_test()
        self.send_challenge()
        # else:
        #     self.logger.info('Nothing to be challenged...')
        self.df = super().run()
        return self.df
