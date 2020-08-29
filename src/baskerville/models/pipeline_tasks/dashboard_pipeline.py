# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.pipeline_tasks.tasks_base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks import GenerateFeatures,  \
    AttackDetection, GetDataLog, Predict


def set_up_dashboard_pipeline(config: BaskervilleConfig):
    task = [
        GetDataLog(
            config,
            steps=[
                GenerateFeatures(config),
                Predict(config),
                AttackDetection(config),
                # Do we need to save this somewhere? e.g. in a test db?
            ]),
    ]

    main_task = Task(config, task)
    main_task.name = 'Dashboard Pipeline'
    return main_task

