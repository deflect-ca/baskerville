# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

FROM equalitie/baskerville:worker

COPY ./src /usr/local/baskerville/src
COPY ./data/jars /usr/local/baskerville/data/jars

COPY ./requirements.txt /usr/local/baskerville
RUN pip3 install -e .