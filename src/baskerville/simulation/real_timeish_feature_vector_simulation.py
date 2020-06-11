# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import datetime
import json
import os
import random
import time
import traceback

from baskerville.util.helpers import get_logger, lines_in_file
from dateutil.tz import tzutc
from pyspark.sql import functions as F, types as T

logger = get_logger(__name__)

COUNTER = 0
SESSION_COUNTER = 0
topic_name = 'predictions'


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
    # time.sleep(30)

    producer = None

    if topic_name:
        from confluent_kafka import Producer
        producer = Producer({'bootstrap.servers': kafka_url})
        active_columns = ['id_client', 'id_group', 'features']

        if not spark:
            from baskerville.spark import get_spark_session
            spark = get_spark_session()
            spark.conf.set('spark.driver.memory', '8G')

        print('Starting...')
        df = spark.read.json(path).cache()
        common_active_cols = [c for c in active_columns if c in df.columns]
        df = df.select(*common_active_cols).sort('id_client')
        print('Dataframe read...')
        TOPIC_BC = spark.sparkContext.broadcast(topic_name)
        KAFKA_URL_BC = spark.sparkContext.broadcast(kafka_url)

        def send_to_kafka(id_client, id_group, features):
            from confluent_kafka import Producer
            producer = Producer({'bootstrap.servers': KAFKA_URL_BC.value})
            time.sleep(random.randrange(5, 20))
            producer.produce(
                TOPIC_BC.value,
                json.dumps(
                    {
                        'id_client': id_client,
                        'id_group': id_group,
                        'features': features
                    }).encode('utf-8')
            )
            producer.poll(2)
            return True

        try:
            df = df.withColumn(
                'sent',
                F.udf(send_to_kafka, T.BooleanType())(*common_active_cols)
            )
            df.show(1000, False)
        except Exception:
            traceback.print_exc()
            pass
        finally:
            if df:
                df.unpersist()
            if spark:
                spark.catalog.clearCache()


if __name__ == '__main__':
    curr_working_dir = os.path.abspath('')
    path_to_raw_logs = f'{curr_working_dir}' \
                       f'/../../../data/samples/sample_vectors/*'
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

# dd = self.data.withColumn('id_client',
#                           F.monotonically_increasing_id()).withColumn(
#     'id_group', F.monotonically_increasing_id())
# dd.select('id_client', 'id_group', 'features').write.format('json').save(
#     '/Users/mariakaranasou/Projects/EQualitie/opensource/baskerville/data/samples/sample_vectors')
