# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.base import Task
from baskerville.models.config import BaskervilleConfig
from baskerville.models.steps import Preprocess, SaveInStorage, \
    Predict, GetDataLog


def set_up_irawlog_pipeline(config: BaskervilleConfig):

    predict_tasks = [
      GetDataLog(
           config,
           steps=[
                  Preprocess(config),
                  Predict(config),
                  SaveInStorage(config),
      ]),
    ]

    iraw_log_pipeline = Task(config, predict_tasks)
    iraw_log_pipeline.name = 'Raw Log Pipeline'
    return iraw_log_pipeline