# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

FROM baskerville/spark-py:v2.4.7

USER ${spark_uid}

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git

RUN pip3 install --no-cache-dir --upgrade pip
WORKDIR /usr/local
RUN git clone --single-branch --branch categorical_features https://github.com/equalitie/spark-iforest.git
WORKDIR /usr/local/spark-iforest/python
RUN python3 setup.py sdist
WORKDIR /usr/local
RUN git clone https://github.com/equalitie/esretriever.git
WORKDIR esretriever
RUN pip3 install .
WORKDIR /usr/local
RUN git clone --single-branch --branch docker https://github.com/equalitie/baskerville.git
WORKDIR /usr/local/baskerville
RUN pip3 install -e .
RUN mkdir -p src/baskerville/logs
