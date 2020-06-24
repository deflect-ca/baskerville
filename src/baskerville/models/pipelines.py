# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import json
import traceback
from datetime import timedelta, datetime
import math

from baskerville.models.base_spark import SparkPipelineBase
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


class ElasticsearchPipeline(SparkPipelineBase):
    """
    A pipeline for processing data directly from an ElasticSearch instance.
    """

    def __init__(
            self, db_conf, els_conf, engine_conf, spark_conf, clean_up=True
    ):
        super(ElasticsearchPipeline, self).__init__(
            db_conf, engine_conf, spark_conf, clean_up
        )

        self.els_conf = els_conf
        self.manual_conf = engine_conf.es_log
        self.start = self.manual_conf.start
        self.stop = self.manual_conf.stop
        self.batch_length = self.manual_conf.batch_length
        self.batch_timedelta = timedelta(minutes=int(self.batch_length))
        self.hosts = None
        if self.manual_conf.hosts:
            self.hosts = ', '.join(self.manual_conf.hosts)
        self.save_logs_dir = self.manual_conf.save_logs_dir
        self.batch_start = self.start
        self.batch_stop = self.batch_start + self.batch_timedelta
        self.batch_i = 1
        self.batch_n = math.ceil(
            float((self.stop - self.start).total_seconds()) /
            (self.batch_length * 60.)
        )

    def initialize(self):
        """
        Start sessions, initialize cache/features/model/dfs.
        :return:
        """
        super().initialize()
        self.set_up_es()

    def run(self):

        while self.batch_start < self.stop:
            self.batch_stop = self.batch_start + self.batch_timedelta

            self.create_runtime()
            self.get_data()
            self.process_data()
            self.reset()
            self.batch_start = self.batch_stop
            self.batch_i += 1

    def create_runtime(self):
        self.runtime = self.tools.create_runtime(
            start=self.batch_start,
            stop=self.batch_stop,
            target_site=self.hosts,
            conf=self.engine_conf,
            comment=f'batch runtime {self.batch_i} of {self.batch_n}'
        )

    def get_data(self):
        from pyspark.sql import functions as F

        filter_condition = (F.col('@timestamp') >= self.runtime.start) & \
                           (F.col('@timestamp') < self.runtime.stop)

        if self.hosts is not None:
            host_filter = (F.col('client_request_host')
                           == self.manual_conf.hosts[0])
            if len(self.manual_conf.hosts) > 1:
                for h in self.manual_conf.hosts[1:]:
                    host_filter = host_filter | (
                        F.col('client_request_host') == h
                    )
            filter_condition = filter_condition & host_filter

        self.logs_df = self.es_storage.get(
            self.runtime.start,
            self.runtime.stop,
            filter_condition=filter_condition,
            extra_config={
                'es.mapping.include': ','.join(
                    self.group_by_cols + self.feature_manager.active_columns
                )
            },
            columns_to_keep=list(
                self.group_by_cols + self.feature_manager.active_columns
            )
        ).select(
            *self.group_by_cols, *self.feature_manager.active_columns
        ).persist(self.spark_conf.storage_level)

        self.logger.info('Will be retrieving {} rows'.format(
            self.logs_df.count()
        )
        )

        if self.save_logs_dir:
            log_name = f'/{self.runtime.start.strftime("%Y-%m-%d-%H%M%S")}' \
                       f'_' + \
                       f'{self.runtime.stop.strftime("%Y-%m-%d-%H%M%S")}'
            if self.runtime.target:
                log_name += f'_{"_".join(self.runtime.target)}' \
                    if isinstance(self.runtime.target, list) \
                    else f'_{self.runtime.target}'
            self.save_logs(self.logs_df, self.save_logs_dir + log_name)

    def set_up_es(self):

        from es_retriever.es.storage import EsStorage
        from es_retriever.config import Config

        self.es_config = Config(
            es_url=self.els_conf.host,
            es_user=self.els_conf.user,
            es_pass=self.els_conf.password,
            es_base_index=self.els_conf.base_index,
            es_index_type=self.els_conf.index_type,
        )
        # todo: fix this in es-retriever: fix setup to include jars
        conf = self.es_config.spark_conf.copy()
        conf['spark.jars'] = self.spark_conf.jars
        self.es_config.spark_conf = conf
        self.es_storage = EsStorage(self.es_config, init_session=False)
        self.es_storage.spark_conf = conf
        self.es_storage.session_getter = self.es_session_getter
        self.es_storage.session_getter()

    def es_session_getter(self):
        from pyspark.sql import SparkSession
        from pyspark import SparkConf

        conf = SparkConf()
        conf.set('spark.logConf', 'true')
        conf.set('spark.jars', self.spark_conf.jars)
        conf.set('spark.driver.memory', '6G')
        conf.set(
            'spark.sql.session.timeZone', self.spark_conf.session_timezone
        )
        conf.set('spark.sql.shuffle.partitions',
                 self.spark_conf.shuffle_partitions)

        spark = SparkSession.builder \
            .config(conf=conf) \
            .appName('Baskerville Spark') \
            .getOrCreate()

        if self.spark_conf.log_level:
            spark.sparkContext.setLogLevel(self.spark_conf.log_level)

        spark.conf.set('spark.jars', self.spark_conf.jars)

        for k, v in self.es_config.es_read_conf.items():
            spark.conf.set(k, v)
        spark.conf.set("es.port", "9200")
        return spark

    def save_logs(self, spark_df, save_logs_path):
        spark_df.coalesce(1).write.mode('overwrite').format('json').save(
            save_logs_path)


class RawLogPipeline(SparkPipelineBase):
    """
    A pipeline that processes a list of raw files.
    """
    log_paths: list

    def __init__(self, db_conf, engine_conf, spark_conf, clean_up=True):
        self.log_paths = engine_conf.raw_log.paths
        super(RawLogPipeline, self).__init__(
            db_conf, engine_conf, spark_conf, clean_up
        )
        self.log_paths = engine_conf.raw_log.paths
        self.batch_i = 1
        self.batch_n = len(self.log_paths)
        self.current_log_path = None

    def run(self):
        for log in self.log_paths:
            self.logger.info(f'Processing {log}...')
            self.current_log_path = log

            self.create_runtime()
            self.get_data()
            self.process_data()
            self.reset()

            self.batch_i += 1

    def create_runtime(self):
        self.runtime = self.tools.create_runtime(
            file_name=self.current_log_path,
            conf=self.engine_conf,
            comment=f'batch runtime {self.batch_i} of {self.batch_n}'
        )
        self.logger.info('Created runtime {}'.format(self.runtime.id))

    def get_data(self):
        """
        Gets the dataframe according to the configuration
        :return: None
        """

        self.logs_df = self.spark.read.json(
            self.current_log_path
        ).persist(self.spark_conf.storage_level)
        # .repartition(*self.group_by_cols)

        self.logger.info('Got dataframe of #{} records'.format(
            self.logs_df.count())
        )
        self.load_test()


class KafkaPipeline(SparkPipelineBase):
    """
    A pipeline that processes data from a Kafka instance every x seconds.
    """

    def __init__(
            self,
            db_conf,
            engine_conf,
            kafka_conf,
            spark_conf,
            clean_up=True
    ):
        super(KafkaPipeline, self).__init__(
            db_conf, engine_conf, spark_conf, clean_up=clean_up,
        )

        self.kafka_conf = kafka_conf
        self.start = None
        self.ssc = None

    def initialize(self):
        super().initialize()
        self.ssc = StreamingContext(
            self.spark.sparkContext, self.engine_conf.time_bucket
        )

    def create_runtime(self):
        self.runtime = self.tools.create_runtime(
            start=self.start,
            conf=self.engine_conf
        )

    def get_data(self):

        self.logs_df = self.logs_df.map(lambda l: json.loads(l[1])).toDF(
            self.data_parser.schema
        ).repartition(*self.group_by_cols).persist(
            self.spark_conf.storage_level)

        self.load_test()

    def run(self):
        self.create_runtime()

        kafkaParams = {
            # 'bootstrap.servers': self.kafka_conf.bootstrap_servers,
            'metadata.broker.list': self.kafka_conf.bootstrap_servers,
            'auto.offset.reset': 'largest',
            # 'security.protocol': self.kafka_conf.security_protocol,
            # 'ssl.truststore.location': self.kafka_conf
            # .ssl_truststore_location,
            # 'ssl.truststore.password': self.kafka_conf
            # .ssl_truststore_password,
            # 'ssl.keystore.type': 'JKS',
            # 'ssl.truststore.type': 'JKS',
            # 'ssl.keystore.location': self.kafka_conf.ssl_keystore_location,
            # 'ssl.keystore.password': self.kafka_conf.ssl_keystore_password,
            # 'ssl.key.password': self.kafka_conf.ssl_key_password,
            # 'ssl.endpoint.identification.algorithm':
            # self.kafka_conf.ssl_endpoint_identification_algorithm,

            'group.id': self.kafka_conf.consume_group,
            # 'auto.create.topics.enable': 'true'
        }

        kafkaStream = KafkaUtils.createDirectStream(
            self.ssc,
            [self.kafka_conf.logs_topic],
            kafkaParams=kafkaParams,
            # fromOffsets={TopicAndPartition(
            # self.kafka_conf.consume_topic, 0): 0}
        )

        # from pympler import muppy, summary, tracker
        # tr = tracker.SummaryTracker()

        def process_subsets(time, rdd):
            global CLIENT_PREDICTION_ACCUMULATOR, CLIENT_REQUEST_SET_COUNT

            self.logger.info('Data until {}'.format(time))
            if not rdd.isEmpty():
                # print('*-' * 25, 'BEFORE', '*-' * 25)
                # all_objects = muppy.get_objects()
                # self.logger.debug(f'**** Length of all objects BEFORE:
                # {len(all_objects)}')
                # tr.print_diff()
                try:
                    # set dataframe to process later on
                    # todo: handle edge cases
                    # todo: what happens if we have a missing column here?
                    # todo: does the time this takes to complete affects the
                    # kafka messages consumption?
                    self.logs_df = rdd
                    self.get_data()
                    self.remaining_steps = list(self.step_to_action.keys())

                    for step, action in self.step_to_action.items():
                        self.logger.info('Starting step {}'.format(step))
                        action()
                        self.logger.info('Completed step {}'.format(step))
                        self.remaining_steps.remove(step)
                    # print('*'*50, 'AFTER', '*'*50)
                    # all_objects = muppy.get_objects()
                    # self.logger.debug(
                    #     f'**** Length of all objects AFTER:
                    #     {len(all_objects)}')
                    # tr.print_diff()
                    self.logger.debug(
                        f'self.spark.sparkContext.'
                        f'_jsc.getPersistentRDDs().items() '
                        f'{ len(self.spark.sparkContext._jsc.getPersistentRDDs().items())}')  # noqa
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


class SparkStructuredStreamingRealTimePipeline(SparkPipelineBase):
    def __init__(
            self,
            db_conf,
            engine_conf,
            kafka_conf,
            spark_conf,
            clean_up=True
    ):
        super(SparkStructuredStreamingRealTimePipeline, self).__init__(
            db_conf,
            engine_conf,
            spark_conf,
            clean_up=clean_up,
        )
        self.engine_conf = engine_conf
        self.features_conf = engine_conf.features
        self.kafka_conf = kafka_conf
        self.log_parser = self.engine_conf.data_config.parser
        self.start = None

        self.ssc = StreamingContext(
            self.spark.sparkContext, self.engine_conf.time_bucket
        )

    def create_runtime(self):
        self.runtime = self.tools.create_runtime(
            start=self.start,
            dt_bucket=self.engine_conf.time_bucket
        )

    def get_data(self):
        self.logs_df = self.logs_df.map(lambda l: json.loads(l[1])).toDF(
            self.log_parser.schema
        )
        # since filtering is done in a next step, do not use actions here,
        # because it will cause calculations with the whole dataset
        # self.logger.info('Got dataframe of #{} records'.format(
        #     self.logs_df.count())
        # )

    def run(self):
        self.start = datetime.utcnow()
        self.create_runtime()

        # Subscribe to a pattern, read from the end of the stream:
        # kafkaStream = self.spark \
        #     .read \
        #     .format("kafka") \
        #     .option("startingOffsets", "earliest") \
        #     .option("kafka.bootstrap.servers", self.kafka_conf.url) \
        #     .option("subscribe", self.kafka_conf.consume_topic) \
        #     .option("auto.offset.reset", "earliest") \
        #     .load()

        # .option("kafka.partition.assignment.strategy", "range") \
        # .option("security.protocol", "SSL") \
        # .option("ssl.truststore.location",
        # "/deflect-analytics-ecosystem/
        # containers/kafka/local_cert/client.truststore.jks") \
        # .option("ssl.truststore.password", "kafkadocker") \
        # .option("ssl.keystore.location",
        # "/deflect-analytics-ecosystem/containers/kafka/
        # local_cert/kafka.server.keystore.jks") \
        # .option("ssl.keystore.password", "kafkadocker") \
        # .option("ssl.key.password", "kafkadocker") \
        # .load()

        # .option("subscribePattern", self.kafka_conf.consume_topic) \

        # kafkaStream = self.spark \
        #     .readStream \
        #     .format("kafka") \
        #     .option("kafka.bootstrap.servers", self.kafka_conf.zookeeper) \
        #     .option("startingOffsets", "earliest") \
        #     .option("subscribe", self.kafka_conf.consume_topic) \
        #     .option("auto.offset.reset", "earliest") \
        #     .option("security.protocol", "SSL") \
        #     .option("ssl.truststore.location",
        #             "/deflect-analytics-ecosystem/containers/
        #             kafka/local_cert/client.truststore.jks") \
        #     .option("ssl.truststore.password", "kafkadocker") \
        #     .option("ssl.keystore.location",
        #             "/deflect-analytics-ecosystem/
        #             containers/kafka/local_cert/kafka.server.keystore.jks") \
        #     .option("ssl.keystore.password", "kafkadocker") \
        #     .option("ssl.key.password", "kafkadocker") \
        #     .load()
        #
        # import pyspark.sql.functions as F
        # df1 = kafkaStream.selectExpr("CAST(value AS STRING)",
        #                     "CAST(timestamp AS TIMESTAMP)"
        #       ).select(F.from_json("value", self.data_parser.schema))
        #
        # q = df1.writeStream.format("console") \
        #         .option("truncate","false")\
        #         .start()\
        #         .awaitTermination()
        # # .option("kafka.partition.assignment.strategy", "range") \
        #
        # NOTE: make sure kafka 8 jar is in the spark.jars, won't work with 10
        # https://elephant.tech/spark-2-0-streaming-from-ssl-kafka-with-hdp-2-4/  # noqa

        # topicPartion = TopicAndPartition(self.kafka_conf.consume_topic, 0)

        kafkaStream = KafkaUtils.createDirectStream(
            self.ssc,
            [self.kafka_conf.logs_topic],
            {
                # 'bootstrap.servers': self.kafka_conf.zookeeper,
                'metadata.broker.list': self.kafka_conf.url,
                # "kafka.sasl.kerberos.service.name": "kafka",
                # "kafka.sasl.kerberos.service.name":
                # "/usr/lib/jvm/jdk1.8.0_162/jre/lib/security/cacerts",
                'group.id': self.kafka_conf.consume_group,
                # 'auto.offset.reset': 'largest',
                # 'security.protocol': self.kafka_conf.security_protocol,
                # "kafka.ssl.truststore.location":
                # self.kafka_conf.ssl_truststore_location,
                # "kafka.ssl.truststore.password":
                # self.kafka_conf.ssl_truststore_password
            }
            # fromOffset={topicPartion: int(0)}
        )

        # readDF = kafkaStream.selectExpr("CAST(key AS STRING)",
        #                            "CAST(value AS STRING)")

        # readDF.show()

        # query = readDF.writeStream.format("console").start()
        # import time
        # time.sleep(10)  # sleep 10 seconds
        # query.stop()
        # windowed_stream = kafkaStream.withWatermark("@timestamp",
        # f"{self.engine_conf.time_bucket} seconds").selectExpr(
        # "CAST(value AS STRING)",
        # "CAST(timestamp AS TIMESTAMP)").toDF('log', 'timestamp')
        #
        # q = kafkaStream.writeStream \
        #     .format("console") \
        #     .option("truncate", "false")

        # q = windowed_stream.count()
        # print(q)
        #
        # windowed_stream.start().awaitTermination()

        def process_subsets(time, rdd):
            self.logger.info('Data until {}'.format(time))
            if not rdd.isEmpty():
                # set dataframe to process later on
                self.logs_df = rdd
                self._get_df()
                self.remaining_steps = list(self.step_to_action.keys())

                for step, action in self.step_to_action.items():
                    self.logger.info('Starting step {}'.format(step))
                    action()
                    self.logger.info('Completed step {}'.format(step))
                    self.remaining_steps.remove(step)
            else:
                self.logger.info('Empty RDD...')
            self.reset()

        kafkaStream.foreachRDD(process_subsets)

        self.ssc.start()

        self.ssc.awaitTermination()
