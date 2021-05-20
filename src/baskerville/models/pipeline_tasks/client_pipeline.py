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
    GetPredictions, MergeWithSensitiveData, RefreshCache, AttackDetection, SendToKafka2, \
    GetDataLog, Predict, Challenge


def set_up_preprocessing_pipeline(config: BaskervilleConfig):
    if config.engine.use_kafka_for_sensitive_data:
        steps = [
                GenerateFeatures(config),
                SendToKafka2(
                    config=config,
                    topic1=config.kafka.features_topic,
                    columns1=('id_client', 'uuid_request_set', 'features'),
                    topic2=config.engine.kafka_topic_sensitive,
                    columns2=(
                        'id_client', 'id_request_sets',
                        'features',
                        'target', 'ip', 'num_requests', 'target_original', 'first_ever_request',
                        'id_runtime', 'time_bucket', 'start', 'stop', 'subset_count', 'dt', 'features')
                ),
                RefreshCache(config)
            ]
    else:
        steps = [
                GenerateFeatures(config),
                CacheSensitiveData(config),
                SendToKafka(
                    config=config,
                    columns=('id_client', 'uuid_request_set', 'features'),
                    topic=config.kafka.features_topic,
                ),
                RefreshCache(config)
            ]
    task = [
        GetDataKafka(
            config,
            steps=steps),
    ]

    main_task = Task(config, task)
    main_task.name = 'Preprocessing Pipeline'
    return main_task


def set_up_client_rawlog_pipeline(config: BaskervilleConfig):
    """
    Reads from raw log and sends features to Kafka for prediction
    Note: this is mostly set up for testing
    """
    task = [
        GetDataLog(
            config,
            steps=[
                GenerateFeatures(config),
                # CacheSensitiveData(config),
                SendToKafka(
                    config=config,
                    columns=('id_client', 'uuid_request_set', 'features'),
                    topic=config.kafka.features_topic,
                ),
                Predict(config),
                AttackDetection(config),
                Challenge(config),
                # Save(config, json_cols=[]),
                # RefreshCache(config),
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
                AttackDetection(config),
                Challenge(config),
                Save(config),
            ]),
    ]

    main_task = Task(config, tasks)
    main_task.name = 'Postprocessing Pipeline'
    return main_task
