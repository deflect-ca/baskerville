# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.pipeline_tasks.tasks_base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks import GetDataKafka, \
    GenerateFeatures, \
    Save, CacheSensitiveData, SendToKafka, \
    GetPredictions, MergeWithSensitiveData, RefreshCache


def set_up_preprocessing_pipeline(config: BaskervilleConfig):
    task = [
        GetDataKafka(
            config,
            steps=[
                GenerateFeatures(config),
                CacheSensitiveData(config),
                SendToKafka(
                    config=config,
                    columns=('id_client', 'id_request_sets', 'features'),
                    topic=config.kafka.features_topic,
                ),
                RefreshCache(config)
            ]),
    ]

    main_task = Task(config, task)
    main_task.name = 'Preprocessing Pipeline'
    return main_task


def set_up_postprocessing_pipeline(config: BaskervilleConfig):
    tasks = [
        GetPredictions(
            config,
            steps=[
                MergeWithSensitiveData(config),
                Save(config, json_cols=[]),
            ]),
    ]

    main_task = Task(config, tasks)
    main_task.name = 'Postprocessing Pipeline'
    return main_task
