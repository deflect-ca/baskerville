# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.pipeline_tasks.tasks_base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks import GetFeatures, \
    SendToKafka, Predict, RefreshModel


def set_up_prediction_pipeline(config: BaskervilleConfig):
    tasks = [
        GetFeatures(
            config,
            steps=[
                Predict(config),
                SendToKafka(
                    config=config,
                    columns=('id_client', 'uuid_request_set', 'prediction', 'score', 'classifier_score'),
                    cc_to_client=True,
                    topic=config.kafka.predictions_topic,
                    client_topic=config.kafka.predictions_topic_client,
                ),
                RefreshModel(config),
            ]),
    ]

    main_task = Task(config, tasks)
    main_task.name = 'Prediction Pipeline'
    return main_task
