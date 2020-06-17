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

from baskerville.util.helpers import get_logger
from pyspark.sql import functions as F, types as T

logger = get_logger(__name__)

COUNTER = 0
SESSION_COUNTER = 0
topic_name = 'predictions'


def simulation(
        path,
        kafka_url='0.0.0.0:9092',
        topic_name='deflect.logs',
        spark=None,
):
    """
    Loads feature vectors with id_client and id_group and publishes them one by
    one with some random delay to the defined topic. This is used
    :param str path: the path to feature vector samples
    :param str kafka_url: the url to kafka, defaults to '0.0.0.0:9092'
    :param str topic_name: the topic name to publish to
    :param pyspark.SparkSession spark: the spark session
    :return: None
    """

    if topic_name:
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
    path_to_raw_logs = os.path.join(
        curr_working_dir,
        '..', '..', '..',
        'data', 'samples', 'sample_vectors',
        '*'
    )
    kafka_url = '0.0.0.0:9092'
    simulation(
        path_to_raw_logs,
        kafka_url,
        topic_name=topic_name,
    )

# data = self.data.withColumn(
#   'id_client',
#   F.monotonically_increasing_id()
# ).withColumn(
#     'id_group', F.monotonically_increasing_id()
# )
# data.select('id_client', 'id_group', 'features').write.format('json').save(
#     '/path/to/baskerville/data/samples/sample_vectors')
