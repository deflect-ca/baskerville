# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from baskerville.models.pipeline_tasks.incident_loader import IncidentLoader
from baskerville.models.pipeline_tasks.tasks_base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks import GetDataPostgres, Train
from baskerville.models.pipeline_tasks.train_classifier import TrainClassifier


def set_up_training_pipeline(config: BaskervilleConfig):
    data_params = config.engine.training.data_parameters

    training_tasks = [
        GetDataPostgres(  # or any other source
            config,
            from_date=data_params.get('from_date'),
            to_date=data_params.get('to_date'),
            training_days=data_params.get('training_days'),
            sampling_percentage=data_params.get('sampling_percentage', 10.0),
            steps=[
                Train(config),
                TrainClassifier(config)
            ]),

    ]

    training_pipeline = Task(config, training_tasks)
    training_pipeline.name = 'Training Pipeline'
    return training_pipeline


def set_up_classifier_training_pipeline(config: BaskervilleConfig):
    training_tasks = [
        IncidentLoader(
            config,
            incident_ids=config.engine.training.classifier_incidents,
            steps=[
                TrainClassifier(config)
            ]),

    ]

    training_pipeline = Task(config, training_tasks)
    training_pipeline.name = 'Training Pipeline'
    return training_pipeline