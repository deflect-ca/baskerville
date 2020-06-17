# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.pipeline_tasks.tasks_base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks import GetPredictionsKafka, \
    PredictionOutput, Predict


def set_up_prediction_pipeline(config: BaskervilleConfig):
    predict_tasks = [
      GetPredictionsKafka(
           config,
           steps=[
                  Predict(config),
                  PredictionOutput(config),
      ]),
    ]

    predict_pipeline = Task(config, predict_tasks)
    predict_pipeline.name = 'Prediction Pipeline'
    return predict_pipeline
