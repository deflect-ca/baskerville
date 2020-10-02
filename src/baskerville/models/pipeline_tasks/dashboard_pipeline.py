# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.pipeline_tasks.tasks_base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks import GenerateFeatures, \
    AttackDetection, GetDataLog, Predict, CacheSensitiveData, SendToKafka, \
    RefreshCache, GetPredictions, MergeWithSensitiveData, Save


def set_up_dashboard_pipeline(config: BaskervilleConfig):
    tasks = [
        GetDataLog(
            config,
            steps=[
                GenerateFeatures(config),
                Predict(config),
                AttackDetection(config),
                # Do we need to save this somewhere? e.g. in a test db?
            ]),
    ]

    main_task = Task(config, tasks)
    main_task.name = 'Dashboard Pipeline'
    return main_task


def set_up_dashboard_preprocessing_pipeline(config: BaskervilleConfig):
    task = [
        GetDataLog(
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
    main_task.name = 'Dashboard Preprocessing Pipeline'
    return main_task


def set_up_dashboard_postprocessing_pipeline(config: BaskervilleConfig):
    tasks = [
        GetPredictions(
            config,
            steps=[
                MergeWithSensitiveData(config),
                AttackDetection(config),
                Save(config, json_cols=[]),
            ]),
    ]

    main_task = Task(config, tasks)
    main_task.name = 'Dashboard Post-processing Pipeline'
    return main_task
