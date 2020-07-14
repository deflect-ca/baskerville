# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.pipeline_tasks.tasks_base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks import GetDataKafka, GenerateFeatures, \
    Save, CacheData, SendFeatures, \
    GetPredictions, MergeWithCachedData


def set_up_preprocessing_pipeline(config: BaskervilleConfig):
    task = [
        GetDataKafka(
            config,
            steps=[
                GenerateFeatures(config),
                SendFeatures(
                    config,
                    output_columns=('id_client', 'id_request_sets', 'features'),
                    client_mode=True
                ),
                CacheData(config),
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
                MergeWithCachedData(config),
                Save(config),
            ]),
    ]

    main_task = Task(config, tasks)
    main_task.name = 'Postprocessing Pipeline'
    return main_task
