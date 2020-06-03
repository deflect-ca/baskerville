from baskerville.models.base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.steps import GetDataKafka, Preprocess, \
    PredictionOutput, SaveInStorage, PredictionInput, Predict


def set_up_iprediction_pipeline(config: BaskervilleConfig):
    predict_tasks = [
      GetDataKafka(
           config,
           steps=[
                  Predict(config),
                  PredictionOutput(config),
                  SaveInStorage(config),
      ]),
    ]

    predict_pipeline = Task(config, predict_tasks)
    predict_pipeline.name = 'Predict Pipeline'
    return predict_pipeline
