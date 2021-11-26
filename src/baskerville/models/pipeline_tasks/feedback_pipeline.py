# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from baskerville.db.dashboard_models import SubmittedFeedback
from baskerville.models.pipeline_tasks.tasks_base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks import GetDataKafka, SaveFeedback, \
    SendToKafka


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
                SaveFeedback(
                    config,
                    table_model=SubmittedFeedback,
                    not_common=(
                        'id_context',
                        'top_uuid_organization'
                    ),
                ),
                SendToKafka(
                    config,
                    ('uuid_organization', 'id_context', 'success'),
                    'feedback',
                    cmd='feedback_center',
                    cc_to_client=True,
                    client_only=True
                )
            ]),
    ]

    main_task = Task(config, tasks)
    main_task.name = 'Feedback Pipeline'
    return main_task
