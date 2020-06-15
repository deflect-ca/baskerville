# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.steps import GetDataKafka, Preprocess, \
    SaveInStorage, SaveInRedis, PredictionInput, PredictionOutput, \
    GetPredictionsClientKafka, RetrieveRsFromRedis


def set_up_client_processing_pipeline(config: BaskervilleConfig):

    client_tasks = [
      GetDataKafka(
           config,
           steps=[
                  Preprocess(config),
                  PredictionOutput(
                      config,
                      output_columns=('id_client', 'id_group', 'features'),
                      client_mode=True
                  ),
                  SaveInRedis(config),
      ]),
    ]

    client_pipeline = Task(config, client_tasks)
    client_pipeline.name = 'Client Pipeline'
    return client_pipeline


def set_up_client_prediction_pipeline(config: BaskervilleConfig):

    client_tasks = [
      GetPredictionsClientKafka(
           config,
           steps=[
                  RetrieveRsFromRedis(config),
                  SaveInStorage(config),
      ]),
    ]

    client_pipeline = Task(config, client_tasks)
    client_pipeline.name = 'Client Pipeline'
    return client_pipeline
