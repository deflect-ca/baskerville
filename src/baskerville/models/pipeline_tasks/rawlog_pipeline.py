# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.pipeline_tasks.tasks_base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks import GenerateFeatures, \
    Save, \
    Predict, GetDataLog, AttackDetection, RefreshCache, Challenge


def set_up_isac_rawlog_pipeline(config: BaskervilleConfig):
    predict_tasks = [
        GetDataLog(
            config,
            steps=[
                GenerateFeatures(config),
                Predict(config),
                AttackDetection(config),
                Challenge(config),
                Save(config),
                # RefreshCache(config),
            ]),
    ]

    isac_raw_log_pipeline = Task(config, predict_tasks)
    isac_raw_log_pipeline.name = 'Raw Log Pipeline'
    return isac_raw_log_pipeline
