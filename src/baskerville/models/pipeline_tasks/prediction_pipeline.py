# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.pipeline_tasks.tasks_base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks import GetFeatures, \
    SendToKafka, Predict


def set_up_prediction_pipeline(config: BaskervilleConfig):
    tasks = [
        GetFeatures(
            config,
            steps=[
                Predict(config),
                SendToKafka(
                    config=config,
                    columns=('id_client', 'id_group', 'prediction', 'score'),
                    topic=config.kafka.predictions_topic,
                    cc_to_client=True,
                ),
            ]),
    ]

    main_task = Task(config, tasks)
    main_task.name = 'Prediction Pipeline'
    return main_task
