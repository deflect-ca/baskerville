# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import datetime
import json
import os
import time
import traceback

import pandas as pd
from baskerville.util.helpers import get_logger, lines_in_file
from dateutil.tz import tzutc

logger = get_logger(__name__)

COUNTER = 0
SESSION_COUNTER = 0
topic_name = 'deflect.logs'


def load_logs(path):
    """
    Load json logs from a path
    :param str path: the path to file.json
    :return: a pandas Dataframe with the logs
    :rtype: pandas.DataFrame
    """
    return pd.read_json(path, orient='records', lines=True, encoding='utf-8')


def simulation(
        path,
        time_window,
        kafka_url='0.0.0.0:9092',
        zookeeper_url='localhost:2181',
        topic_name='deflect.logs',
        sleep=True,
        verbose=False,
        spark=None,
        use_spark=False
):
    """
    Loads raw logs, groups them by the defined time window and publishes
    the grouped raw logs in Kafka if a producer is given, else, it prints out
    the groups. After publishing the logs line by line, it will sleep for the
    x remaining seconds of the time window if any.
    :param str path: the path to raw logs as they are stored in ELS
    :param timedelta time_window: the time window for the interval
    :param str kafka_url: the url to kafka, defaults to '0.0.0.0:9092'
    :param str zookeeper_url: the url to zookeeper, defaults to
    'localhost:2181'
    :param bytes topic_name: the topic name to publish to
    :param bool sleep: if True, the program will sleep after publishing each
    group of time windowed logs, for the remaining seconds until a time window
    is complete.
    :param bool verbose: verbose flag
    :return: None
    """

    # a short delay for warming up the pipeline
    time.sleep(30)

    producer = None

    if topic_name:
        from confluent_kafka import Producer
        producer = Producer({'bootstrap.servers': kafka_url})

    if not use_spark and lines_in_file(path) < 1e6:
        # pandas can usually handle well files under 1M lines - but that
        # depends on the machine running the script (amount of RAM)
        df = load_logs(path)
        publish_df_split_in_time_windows(
            time_window, producer, topic_name, df, verbose, sleep
        )
    else:
        from pyspark.sql import functions as F

        active_columns = [
            '@timestamp', 'timestamp', 'client_request_host', 'client_ip',
            'client_ua', 'client_url', 'content_type',
            'http_response_code', 'querystring',
            'reply_length_bytes'
        ]

        if not spark:
            from baskerville.spark import get_spark_session
            spark = get_spark_session()
            spark.conf.set('spark.driver.memory', '8G')

        print('Starting...')
        df = spark.read.json(path).cache()
        df = df.withColumn('timestamp', F.col('@timestamp').cast('timestamp'))
        common_active_cols = [c for c in active_columns if c in df.columns]
        df = df.select(*common_active_cols).sort('@timestamp')
        print('Dataframe read...')

        min_max_df = df.agg(
            F.min(F.col('timestamp')).alias('min_ts'),
            F.max(F.col('timestamp')).alias('max_ts')
        ).collect()[0]
        current_window = min_max_df[0]
        max_window = min_max_df[1]
        window_df = None

        try:
            while True:
                filter_ = (
                    (F.col('timestamp') >= current_window) &
                    (F.col('timestamp') <= current_window + time_window)
                )
                if verbose:
                    logger.info(f'Current window: {current_window}, '
                                f'Max window: {max_window}')
                    logger.info(f'Running for {str(filter_._jc)}')
                window_df = df.select(
                    *common_active_cols).where(filter_).cache()
                pandas_df = window_df.toPandas()
                if not pandas_df.empty:
                    publish_df_split_in_time_windows(
                        time_window, producer, topic_name, pandas_df, verbose, sleep
                    )
                current_window = current_window + time_window
                logger.info(f'{current_window} {max_window} {time_window}')
                if current_window > max_window:
                    logger.info(
                        f'>> EOF for Simulation, {current_window} {max_window}'
                    )
                    break
        except Exception:
            traceback.print_exc()
            pass
        finally:
            if df:
                df.unpersist()
            if window_df:
                window_df.unpersist()
            if spark:
                spark.catalog.clearCache()


def publish_df_split_in_time_windows(
        time_window, producer, topic_name, df, verbose=False, sleep=True
):
    """
    Publish the dataframe split in time_window seconds.
    :param int time_window: the duration of the time window in seconds
    :param confluent_kafka.Producer producer: the kafka producer
    :topic_name the kafka topic_name
    :param pandas.DataFrame df: the dataframe to publish
    :param boolean verbose:
    :param boolean sleep:if True, sleep for the remaining of the time window
    seconds
    :return: None
    """
    global COUNTER, SESSION_COUNTER

    # load logs and set the timestamp index
    df = df.set_index(pd.DatetimeIndex(df['@timestamp']))
    df.index = pd.to_datetime(df['@timestamp'], utc=True)

    # sort by time
    df.sort_index(inplace=True)

    # group by timeframe - supporting minutes for now
    groupped_df = df.groupby(pd.Grouper(freq=time_window))

    for group in groupped_df:
        time_start = datetime.datetime.now(tz=tzutc())

        request_sets_df = group[1].groupby(
            ['client_request_host', 'client_ip']
        )
        len_request_sets = len(request_sets_df)
        SESSION_COUNTER += len_request_sets
        request_sets_df = None

        json_lines = json.loads(group[1].to_json(orient='records'))
        num_lines = len(json_lines)
        COUNTER += num_lines

        if verbose:
            logger.info('=' * 60)
            logger.info(f'request_set count: {len_request_sets}')
            logger.info(f'request_set count so far: {SESSION_COUNTER}')

        for line in json_lines:
            if producer is not None:
                producer.produce(
                    topic_name, json.dumps(line).encode('utf-8')
                )
                producer.poll(2)

        t_elapsed = datetime.datetime.now(tz=tzutc()) - time_start

        if verbose:
            logger.info('-' * 60)
            logger.info(f'>> Line count in this batch: {num_lines}')
            logger.info(f'>> Line count so far: {COUNTER}')

            logger.info(f'* Started at:{time_start}')
            logger.info(f'* Time elapsed: {t_elapsed}')
            logger.info(f'* Time window: {time_window}')
            logger.info('=' * 60)

        if sleep and t_elapsed < time_window:
            sleep_time = time_window - t_elapsed
            if verbose:
                logger.info(
                    f'Going to sleep for {sleep_time.total_seconds()} '
                    f'seconds...'
                )
            time.sleep(sleep_time.total_seconds())


if __name__ == '__main__':
    curr_working_dir = os.path.abspath('')
    path_to_raw_logs = f'{curr_working_dir}' \
                       f'/../../../data/samples/test_data_1k.json'
    time_window = datetime.timedelta(seconds=120)
    kafka_url = '0.0.0.0:9092'
    simulation(
        path_to_raw_logs,
        time_window,
        kafka_url,
        topic_name=topic_name,
        verbose=True,
        sleep=True,
        use_spark=True
    )
