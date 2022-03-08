# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import StandardScaler, StandardScalerModel, StringIndexer, StringIndexerModel
from pyspark.ml.pipeline import PipelineModel, Pipeline
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

    def __init__(self, features=[]):
        super().__init__()
        self.model = None
        self.features = features

    def evaluate(self, df):
        P = df.where(F.col('label') == 1).count()
        N = df.where(F.col('label') == 0).count()
        FP = df.where((F.col('label') == 0) & (F.col('prediction') == 1)).count()
        TP = df.where((F.col('label') == 1) & (F.col('prediction') == 1)).count()

        self.logger.info(f'Positives = {P}')
        self.logger.info(f'Negatives = {N}')
        self.logger.info(f'Recall={TP / P if P > 0 else -1}')
        self.logger.info(f'FPR={FP / N if N > 0 else -1}')

    def train(self, df):
        df = self.prepare_df(df)

        (trainingData, testData) = df.randomSplit([0.8, 0.2])

        classifier = GBTClassifier(
            labelCol="label",
            featuresCol="features_sparse",
            maxIter=100)
        pipeline = Pipeline(stages=[classifier])
        self.model = pipeline.fit(trainingData)
        predictions_test = self.model.transform(testData)
        predictions_train = self.model.transform(trainingData)
        self.logger.info("Training performance:")
        self.evaluate(predictions_train)
        self.logger.info("Test performance:")
        self.evaluate(predictions_test)

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

    def _get_params_path(self, path):
        return os.path.join(path, 'params.json')

    def _get_model_path(self, path):
        return os.path.join(path, 'model/')

    def save(self, path, spark_session=None, training_config=None):
        file_manager = FileManager(path, spark_session)
        file_manager.save_to_file(self.get_params(), self._get_params_path(path), format='json')
        self.model.save(self._get_model_path(path))

    def load(self, path, spark_session=None):
        file_manager = FileManager(path, spark_session)
        params = file_manager.load_from_file(self._get_params_path(path), format='json')
        self.set_params(**params)

        self.model = PipelineModel.load(self._get_model_path(path))
        return self
