# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.pipeline_tasks.tasks_base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks import GetDataKafka, ReTrain
from baskerville.models.pipeline_tasks.training_pipeline import \
    set_up_training_pipeline


def set_up_retraining_pipeline(config: BaskervilleConfig):
    training_pipeline = set_up_training_pipeline(config)
    training_tasks = [
        GetDataKafka(
            config,
            steps=[ReTrain(config), *training_pipeline.steps],
        )
    ]

    training_pipeline = Task(config, training_tasks)
    training_pipeline.name = 'Training Pipeline'
    return training_pipeline
