# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import json
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


if __name__ == '__main__':
    # https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html
    # https://spark.apache.org/docs/2.1.0/streaming-kafka-0-10-integration.html
    conf = SparkConf()
    curr_working_dir = os.path.abspath('')
    conf.set(
        'spark.jars',
        curr_working_dir +
        '/../../../data/jars/spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar'
    )

    interval = 5  # seconds
    spark = SparkSession.builder \
        .config(conf=conf) \
        .appName('Test') \
        .getOrCreate()
    ssc = StreamingContext(spark.sparkContext, interval)

    kafkaStream = KafkaUtils.createStream(
        ssc, 'localhost:2181', groupId='baskerville', topics={'deflect.logs': 1}
    )

    def process_requests(time, rdd):
        print('Data until {}'.format(time))
        if not rdd.isEmpty():
            df = rdd.map(lambda l: json.loads(l[1])).toDF()
            print(df.show())
            pandas_df = df.toPandas()
            print(pandas_df.head())
        else:
            print('Empty RDD...')

    kafkaStream.foreachRDD(process_requests)
    ssc.start()
    ssc.awaitTermination()
