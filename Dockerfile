# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

FROM mazhurin/baskerville:worker

ENV BASKERVILLE_ROOT=/usr/local/baskerville
ENV POSTGRES_HOST=213.108.110.212
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASS=zUXtnClLrDHgSQ4KK6iiT3
ENV KAFKA_HOST=kafka-0.kafka-headless.default.svc.cluster.local:9092,kafka-1.kafka-headless.default.svc.cluster.local:9092,kafka-2.kafka-headless.default.svc.cluster.local:9092
ENV HADOOP_USER_NAME=hdfs
ENV PYSPARK_PYTHON=python3.7

COPY ./src /usr/local/baskerville/src
COPY ./conf /usr/local/baskerville/conf
COPY ./data/jars /usr/local/baskerville/data/jars