# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import os

from pyspark import SparkConf, StorageLevel
from pyspark.sql import SparkSession


def get_or_create_spark_session(spark_conf):
    """
    Returns a configured spark session
    :param SparkConfig spark_conf: the spark configuration
    :return:
    :rtype: pyspark.Session
    """
    # https://spark.apache.org/docs/latest/tuning.html
    conf = SparkConf()
    conf.set('spark.logConf', 'true')
    conf.set('spark.jars', spark_conf.jars)
    conf.set('spark.master', spark_conf.master)
    conf.set("spark.hadoop.dfs.client.use.datanode.hostname", 'true')
    if spark_conf.redis_host:
        conf.set('spark.redis.host', spark_conf.redis_host)
        conf.set('spark.redis.port', spark_conf.redis_port)
        conf.set('spark.redis.auth', spark_conf.redis_password)

    if spark_conf.spark_executor_instances:
        conf.set('spark.executor.instances',
                 spark_conf.spark_executor_instances)
        # conf.set('spark.streaming.dynamicAllocation.minExecutors', spark_conf.spark_executor_instances)
    if spark_conf.spark_executor_cores:
        conf.set('spark.executor.cores', spark_conf.spark_executor_cores)
    if spark_conf.spark_executor_memory:
        conf.set('spark.executor.memory', spark_conf.spark_executor_memory)
    # todo: https://stackoverflow.com/questions/
    #  49672181/spark-streaming-dynamic-allocation-do-not-remove-executors-in-middle-of-window
    # https://medium.com/@pmatpadi/spark-streaming-dynamic-scaling-and-backpressure-in-action-6ebdbc782a69

    # security
    # https://spark.apache.org/docs/latest/security.html
    # note that: The same secret is shared by all Spark applications and
    # daemons in that case, which limits the security of these deployments,
    # especially on multi-tenant clusters.
    if spark_conf.auth_secret:
        conf.set('spark.authenticate', 'true')
        conf.set('spark.authenticate.secret', spark_conf.auth_secret)

    if spark_conf.ssl_enabled:
        conf.set('spark.ssl.enabled', 'true')
        conf.set('spark.network.crypto.saslFallback', 'false')
        conf.set('spark.network.crypto.enabled', 'true')
        conf.set('spark.ssl.trustStore', spark_conf.ssl_truststore)
        conf.set('spark.ssl.trustStorePassword', spark_conf.ssl_truststore_password)
        conf.set('spark.ssl.keyStore', spark_conf.ssl_keystore)
        conf.set('spark.ssl.keyStorePassword', spark_conf.ssl_keystore_password)
        conf.set('spark.ssl.keyPassword', spark_conf.ssl_keypassword)

        conf.set('spark.ssl.ui.enabled', spark_conf.ssl_ui_enabled)
        conf.set('spark.ssl.standalone.enabled', spark_conf.ssl_standalone_enabled)
        conf.set('spark.ssl.historyServer.enabled', spark_conf.ssl_history_server_enabled)

    # conf.set('spark.streaming.dynamicAllocation.enabled', 'true')
    conf.set('spark.streaming.unpersist', 'true')
    conf.set('spark.sql.session.timeZone', 'UTC')
    # conf.set('spark.dynamicAllocation.enabled', 'true')
    # conf.set('spark.shuffle.service.enabled', 'true')
    # conf.set('spark.streaming.dynamicAllocation.executorIdleTimeout', '3s')
    # conf.set('spark.streaming.dynamicAllocation.initialExecutors', '4')
    # conf.set('spark.streaming.dynamicAllocation.minExecutors', '3')
    conf.set('spark.cleaner.periodicGC.interval', '1m')
    if spark_conf.storage_level == StorageLevel.OFF_HEAP:
        conf.set('spark.memory.offHeap.enabled', 'true')
        conf.set('spark.memory.offHeap.size', spark_conf.off_heap_size or '2g')
    # conf.set('spark.python.worker.memory', '1g')
    conf.set('spark.executor.logs.rolling.strategy', 'time')
    conf.set('spark.executor.logs.rolling.time.interval', 'daily')
    conf.set('spark.python.worker.reuse', 'true')
    conf.set('spark.ui.port', '4042')
    # conf.set('spark.python.profile', 'true')
    conf.set('spark.rdd.compress', 'true')
    conf.set('spark.broadcast.compress', 'true')

    conf.set('spark.history.fs.cleaner.enabled', 'true')
    conf.set('spark.history.fs.cleaner.interval', '1d')
    conf.set('log4j.appender.rolling.maxFileSize', '50MB')
    conf.set('log4j.appender.rolling.maxBackupIndex', '5')
    conf.set('spark.executor.logs.rolling.maxRetainedFiles', '20')
    conf.set('spark.executor.logs.rolling.enableCompression', 'true')
    conf.set('spark.executor.logs.rolling.maxSize', '50MB')

    # ui
    conf.set('spark.ui.retainedJobs', '20')
    conf.set('spark.ui.retainedStages', '20')
    conf.set('spark.ui.retainedTask', '50')
    conf.set('spark.worker.ui.retainedExecutors', '20')
    conf.set('spark.worker.ui.retainedDrivers', '20')
    conf.set('spark.sql.ui.retainedExecutions', '20')
    conf.set('spark.streaming.ui.retainedBatches', '20')

    # parquet related properties
    # conf.set('spark.hadoop.native.lib', 'false')
    # conf.set('spark.sql.parquet.cacheMetadata', 'false')
    conf.set('spark.sql.parquet.enable.dictionary', 'true')
    conf.set('spark.hadoop.parquet.enable.summary-metadata', 'false')
    conf.set('spark.sql.parquet.mergeSchema', 'false')
    conf.set('spark.sql.parquet.filterPushdown', 'true')
    conf.set('spark.sql.hive.metastorePartitionPruning', 'true')

    # https://spark.apache.org/docs/latest/monitoring.html
    # To view the web UI after the app has terminated
    conf.set('spark.eventLog.enabled', spark_conf.event_log)
    conf.set('spark.eventLog.compress', 'true')
    conf.set('spark.eventLog.overwrite', 'true')
    conf.set('spark.ui.dagGraph.retainedRootRDDs', 100000)
    if not os.path.exists('/tmp/spark-events'):
        os.makedirs('/tmp/spark-events')
        conf.set('spark.eventLog.dir', '/tmp/spark-events')
    if spark_conf.spark_executor_cores:
        conf.set('spark.executor.cores', spark_conf.spark_executor_cores)
    if spark_conf.spark_executor_instances:
        conf.set('spark.executor.instances',
                 spark_conf.spark_executor_instances)
    if spark_conf.spark_executor_memory:
        conf.set('spark.executor.memory', spark_conf.spark_executor_memory)
    if spark_conf.serializer:
        conf.set('spark.serializer', spark_conf.serializer)
        if 'KryoSerializer' in spark_conf.serializer:
            if spark_conf.kryoserializer_buffer_max:
                conf.set(
                    'spark.kryoserializer.buffer.max',
                    spark_conf.kryoserializer_buffer_max
                )
            if spark_conf.kryoserializer_buffer:
                conf.set(
                    'spark.kryoserializer.buffer',
                    spark_conf.kryoserializer_buffer
                )
    if spark_conf.driver_extra_class_path:
        conf.set('spark.driver.extraClassPath',
                 spark_conf.driver_extra_class_path)
    if spark_conf.metrics_conf:
        conf.set('spark.metrics.conf', spark_conf.metrics_conf)
    if spark_conf.jars_repositories:
        conf.set('spark.jars.repositories', spark_conf.jars_repositories)
    if spark_conf.jar_packages:
        conf.set('spark.jars.packages', spark_conf.jar_packages)
    # when on a local machine with less than 36GB of ram -XX:+UseCompressedOops
    if spark_conf.executor_extra_java_options:
        conf.set(
            'spark.executor.extraJavaOptions',
            spark_conf.executor_extra_java_options
        )
    if spark_conf.driver_java_options:
        conf.set(
            'spark.driver.java.options', spark_conf.driver_java_options
        )
    if spark_conf.spark_driver_memory:
        conf.set(
            'spark.driver.memory', spark_conf.spark_driver_memory
        )
    conf.set(
        'spark.sql.session.timeZone', spark_conf.session_timezone
    )
    conf.set('spark.sql.shuffle.partitions', spark_conf.shuffle_partitions)
    # conf.set('spark.sql.autoBroadcastJoinThreshold', 1024*1024*100)  # 100MB
    # https://issues.apache.org/jira/browse/SPARK-25998
    conf.set('spark.sql.autoBroadcastJoinThreshold', -1)  # disable

    spark = SparkSession.builder \
        .config(conf=conf) \
        .appName(spark_conf.app_name) \
        .getOrCreate()

    if spark_conf.log_level:
        spark.sparkContext.setLogLevel(spark_conf.log_level)
    return spark


def get_spark_session():
    return SparkSession.builder.getOrCreate()
