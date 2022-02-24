# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from pyspark.ml.feature import StandardScaler, StandardScalerModel, StringIndexer, StringIndexerModel
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql.functions import array
from baskerville.models.model_interface import ModelInterface
from baskerville.spark.helpers import map_to_array, StorageLevelFactory
from baskerville.spark.udfs import udf_to_dense_vector, udf_add_to_dense_vector
from pyspark_iforest.ml.iforest import IForest, IForestModel
import os

from baskerville.util.file_manager import FileManager
from pyspark.sql import functions as F
from pyspark.ml.linalg import SparseVector, VectorUDT
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
import numpy as np


def to_sparse(c):
    def to_sparse_(v):
        if isinstance(v, SparseVector):
            return v
        vs = v
        nonzero = np.nonzero(vs)[0]
        return SparseVector(len(v), nonzero, [d for d in vs if d != 0])

    return F.udf(to_sparse_, VectorUDT())(c)


class ClassifierModel(ModelInterface):

    def __init__(self, features, logger):
        super().__init__()
        self.model = None
        self.features = features
        self.logger = logger

    def train(self, df):
        pass

    def prepare_df(self, df):
        df = map_to_array(df, map_col='features', array_col='features_vector',
                          map_keys=self.features,
                          )
        df = df.withColumn('features_sparse', to_sparse('features_vector'))
        return df

    def predict(self, df):
        df = self.prepare_df(df)
        self.logger.info('GBT transform ...')
        df = self.model.transform(df)
        second_element = udf(lambda v: float(v[1]), FloatType())
        df = df.withColumn('classifier_score', second_element('probability'))
        df = df.drop('features_sparse', 'rawPrediction', 'probability', 'prediction')
        return df

    def save(self, path, spark_session=None, training_config=None):
        pass

    def load(self, path, spark_session=None):
        self.model = PipelineModel.load(path)
        return self
