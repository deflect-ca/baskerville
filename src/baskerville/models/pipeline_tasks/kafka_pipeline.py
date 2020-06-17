# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.pipeline_tasks.tasks_base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks import Preprocess, \
    SaveRsInPostgres, Predict, GetDataKafka


def set_up_isac_kafka_pipeline(config: BaskervilleConfig):
    kafka_tasks = [
        GetDataKafka(
            config,
            steps=[
                Preprocess(config),
                Predict(config),
                SaveRsInPostgres(config),
            ]),
    ]

    kafka_pipeline = Task(config, kafka_tasks)
    kafka_pipeline.name = 'Kafka Pipeline'
    return kafka_pipeline
