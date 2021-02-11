# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from baskerville.db.dashboard_models import Feedback
from baskerville.models.pipeline_tasks.tasks_base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks import GetDataKafka, Save


def set_up_feedback_pipeline(config: BaskervilleConfig):
    """
    Feedback Pipeline listens to a kafka topic and every time_bucket time,
    it gathers all the feedback and saves it to the Feedback table.
    It is considered a low needs pipeline, since feedback is not going to be
    coming in constantly, but rarely.
    In config, kafka-> data_topic should be configured for the data input
    """
    tasks = [
        GetDataKafka(
            config,
            steps=[
                Save(
                    config,
                    table_model=Feedback,
                    not_common=()
                ),
            ]),
    ]

    main_task = Task(config, tasks)
    main_task.name = 'Feedback Pipeline'
    return main_task
