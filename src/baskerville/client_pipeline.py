from baskerville.models.base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.steps import GetDataKafka, Preprocess, \
    PredictionOutput, SaveInStorage, PredictionInput


def set_up_client_processing_pipeline(config: BaskervilleConfig):

    client_tasks = [
      GetDataKafka(
           config,
           steps=[
                  Preprocess(config),
                  PredictionOutput(config),
                  SaveInStorage(config),
      ]),
      PredictionInput(config)
    ]

    client_pipeline = Task(config, client_tasks)
    client_pipeline.name = 'Client Pipeline'
    return client_pipeline


def set_up_client_prediction_pipeline(config: BaskervilleConfig):

    client_tasks = [
      GetDataKafka(
           config,
           steps=[
                  PredictionInput(config),
                  SaveInStorage(config),
      ]),
    ]

    client_pipeline = Task(config, client_tasks)
    client_pipeline.name = 'Client Pipeline'
    return client_pipeline
