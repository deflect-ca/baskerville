from baskerville.models.base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.steps import SaveInStorage, GetDataPostgres, Train, \
    Evaluate, ModelUpdate


def set_up_itraining_pipeline(config: BaskervilleConfig):
    training_tasks = [
      GetDataPostgres(  # or any other source
           config,
           steps=[
                  Train(config),
                  Evaluate(config),
                  SaveInStorage(config),
                  ModelUpdate(config),
      ]),

    ]

    training_pipeline = Task(config, training_tasks)
    training_pipeline.name = 'Training Pipeline'
    return training_pipeline
