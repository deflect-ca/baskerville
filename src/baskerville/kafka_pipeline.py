from baskerville.models.base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.steps import Preprocess, SaveInStorage, \
    Predict, GetDataLog, GetDataKafka


def set_up_ikafka_pipeline(config: BaskervilleConfig):

    kafka_tasks = [
      GetDataKafka(
           config,
           steps=[
                  Preprocess(config),
                  Predict(config),
                  SaveInStorage(config),
      ]),
    ]

    kafka_pipeline = Task(config, kafka_tasks)
    kafka_pipeline.name = 'Kafka Pipeline'
    return kafka_pipeline