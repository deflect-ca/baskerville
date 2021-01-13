# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

FROM baskerville/worker:latest

ENV BASKERVILLE_ROOT=/usr/local/baskerville
ENV POSTGRES_HOST=timescale
ENV POSTGRES_USER=admin
ENV POSTGRES_PASS=weFUtkXHZl0wQuxMBjcHYPxJjfH1AHsv
ENV KAFKA_HOST=bnode1.deflect.ca:9092,bnode2.deflect.ca:9092,bnode3.deflect.ca:9092
ENV HADOOP_USER_NAME=hdfs

COPY ./src /usr/local/baskerville/src
COPY ./conf /usr/local/baskerville/conf