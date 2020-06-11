from baskerville.models.base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.steps import GetPredictionsKafka, \
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
