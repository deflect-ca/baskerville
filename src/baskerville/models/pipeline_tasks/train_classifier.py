# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from pyspark.ml.linalg import SparseVector, VectorUDT

from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks_base import Task
from baskerville.spark.helpers import map_to_array
from baskerville.util.helpers import instantiate_from_str, get_classifier_model_path
import os
import numpy as np
import pyspark.sql.functions as F


def to_sparse(c):
    def to_sparse_(v):
        if isinstance(v, SparseVector):
            return v
        vs = v
        nonzero = np.nonzero(vs)[0]
        return SparseVector(len(v), nonzero, [d for d in vs if d != 0])

    return F.udf(to_sparse_, VectorUDT())(c)


class TrainClassifier(Task):

    def __init__(
            self,
            config: BaskervilleConfig,
            steps: list = (),
    ):
        super().__init__(config, steps)
        self.model = None
        self.training_conf = self.config.engine.training
        self.engine_conf = self.config.engine

    def save(self):
        model_path = get_classifier_model_path(self.engine_conf.storage_path, self.model.__class__.__name__)
        self.model.save(path=model_path, spark_session=self.spark)
        self.logger.debug(f'The new classifier model has been saved to: {model_path}')

    def run(self):
        self.model = instantiate_from_str(self.training_conf.classifier_model)

        params = self.training_conf.classifier_parameters
        self.model.set_params(**params)
        self.model.set_logger(self.logger)

        self.model.train(self.df)

        self.save()
