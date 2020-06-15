# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.steps import Preprocess, SaveInStorage, \
    Predict, GetDataKafka


def set_up_isac_kafka_pipeline(config: BaskervilleConfig):

    kafka_tasks = [
      GetDataKafka(
           config,
           steps=[
                  Preprocess(config),
                  Predict(config),
                  SaveInStorage(config),
      ]),
    ]

    kafka_pipeline = Task(config, kafka_tasks)
    kafka_pipeline.name = 'Kafka Pipeline'
    return kafka_pipeline