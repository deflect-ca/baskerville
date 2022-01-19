# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

FROM equalitie/baskerville:notebook_base

COPY ./src /usr/local/baskerville/src
COPY ./data/jars /usr/local/baskerville/data/jars

WORKDIR /usr/local
RUN apt install wget
RUN wget https://builds.openlogic.com/downloadJDK/openlogic-openjdk/8u262-b10/openlogic-openjdk-8u262-b10-linux-x64.tar.gz
RUN mkdir jdk262
RUN tar -zxvf openlogic-openjdk-8u262-b10-linux-x64.tar.gz -C jdk262

ENV JAVA_HOME=/usr/local/jdk262/openlogic-openjdk-8u262-b10-linux-64

WORKDIR /usr/local/baskerville
RUN pip install  .

