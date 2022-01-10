# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

FROM equalitie/baskerville:spark247

USER ${spark_uid}

RUN apt-get update --allow-releaseinfo-change && \
    apt-get upgrade -y && \
    apt-get install -y git

RUN pip3 install --no-cache-dir --upgrade pip
WORKDIR /usr/local
RUN git clone --single-branch --branch categorical_features https://github.com/equalitie/spark-iforest.git
WORKDIR /usr/local/spark-iforest/python
RUN python3 setup.py sdist
RUN pip3 install dist/pyspark-iforest-2.4.0.99.tar.gz
WORKDIR /usr/local
RUN git clone https://github.com/equalitie/esretriever.git

WORKDIR esretriever
RUN rm requirements.txt && \
    echo 'pyspark==2.4.7' >> requirements.txt && \
    echo 'python-dateutil==2.7.3' >> requirements.txt && \
    echo 'requests==2.20.0' >> requirements.txt

RUN apt-get update -y
RUN pip install numpy
RUN pip install pandas

RUN pip3 install .
WORKDIR /usr/local
RUN git clone --single-branch --branch spark247 https://github.com/equalitie/baskerville.git
WORKDIR /usr/local/baskerville

RUN apt-get -y install python3-scipy

RUN pip3 install -e .
RUN mkdir -p src/baskerville/logs

ENV BASKERVILLE_ROOT=/usr/local/baskerville
ENV HADOOP_USER_NAME=hdfs
ENV PYSPARK_PYTHON=python3.7
ENV PYSPARK_DRIVER_PYTHON=python3.7
