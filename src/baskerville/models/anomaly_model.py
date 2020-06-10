# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from pyspark.ml.feature import StandardScaler, StandardScalerModel, StringIndexer, StringIndexerModel
from pyspark.sql.functions import array

from baskerville.models.model_interface import ModelInterface
from baskerville.spark.helpers import map_to_array, StorageLevelFactory
from baskerville.spark.udfs import udf_to_dense_vector, udf_add_to_dense_vector
from pyspark_iforest.ml.iforest import IForest, IForestModel
import os

from baskerville.util.file_manager import FileManager


class AnomalyModel(ModelInterface):

    def __init__(self, feature_map_column='features',
                 features=None,
                 categorical_features=[],
                 prediction_column="prediction",
                 threshold=0.5,
                 score_column="score",
                 num_trees=100, max_samples=1.0, max_features=1.0, max_depth=10,
                 contamination=0.1, bootstrap=False, approximate_quantile_relative_error=0.,
                 seed=777,
                 scaler_with_mean=False, scaler_with_std=True,
                 storage_level='OFF_HEAP'):
        super().__init__()
        self.prediction_column = prediction_column
        self.score_column = score_column
        self.num_trees = num_trees
        self.max_samples = max_samples
        self.max_features = max_features
        self.max_depth = max_depth
        self.contamination = contamination
        self.bootstrap = bootstrap
        self.approximate_quantile_relative_error = approximate_quantile_relative_error
        self.seed = seed
        self.scaler_with_mean = scaler_with_mean
        self.scaler_with_std = scaler_with_std
        self.features = features
        self.categorical_features = categorical_features
        self.feature_map_column = feature_map_column
        self.storage_level = storage_level

        self.scaler_model = None
        self.iforest_model = None
        self.threshold = threshold
        self.indexes = None
        self.features_values_column = 'features_values'
        self.features_values_scaled = 'features_values_scaled'

    def build_features_vectors(self, df):
        res = map_to_array(
            df,
            map_col=self.feature_map_column,
            array_col=self.features_values_column,
            map_keys=self.features
        ).persist(StorageLevelFactory.get_storage_level(self.storage_level))
        df.unpersist()

        return res.withColumn(
            self.features_values_column,
            udf_to_dense_vector(self.features_values_column)
        )

    def _create_indexes(self, df):
        self.indexes = []
        for c in self.categorical_features:
            indexer = StringIndexer(inputCol=c, outputCol=f'{c}_index') \
                .setHandleInvalid('keep') \
                .setStringOrderType('alphabetAsc')
            self.indexes.append(indexer.fit(df))

    def _add_categorical_features(self, df, feature_column):
        index_columns = []
        for index_model in self.indexes:
            df = index_model.transform(df)
            index_columns.append(index_model.getOutputCol())

        df = df.withColumn('features_all', udf_add_to_dense_vector(
            feature_column, array(*index_columns))) \
            .drop(*index_columns, feature_column) \
            .withColumnRenamed('features_all', feature_column)
        return df

    def train(self, df):
        df = self.build_features_vectors(df)

        scaler = StandardScaler()
        scaler.setInputCol(self.features_values_column)
        scaler.setOutputCol(self.features_values_scaled)
        scaler.setWithMean(self.scaler_with_mean)
        scaler.setWithStd(self.scaler_with_std)
        self.scaler_model = scaler.fit(df)
        df = self.scaler_model.transform(df).persist(
            StorageLevelFactory.get_storage_level(self.storage_level)
        )
        if len(self.categorical_features):
            self._create_indexes(df)
            self._add_categorical_features(df, self.features_values_scaled)

        iforest = IForest(
            featuresCol=self.features_values_scaled,
            predictionCol=self.prediction_column,
            # anomalyScore=self.score_column,
            numTrees=self.num_trees,
            maxSamples=self.max_samples,
            maxFeatures=self.max_features,
            maxDepth=self.max_depth,
            contamination=self.contamination,
            bootstrap=self.bootstrap,
            approxQuantileRelativeError=self.approximate_quantile_relative_error,
            # numCategoricalFeatures=len(self.categorical_features)
        )
        iforest.setSeed(self.seed)
        params = {'threshold': self.threshold}
        self.iforest_model = iforest.fit(df, params)
        df.unpersist()

    def predict(self, df):
        df = self.build_features_vectors(df)
        df = self.scaler_model.transform(df)
        if len(self.categorical_features):
            df = self._add_categorical_features(
                df, self.features_values_scaled
            )
        df = self.iforest_model.transform(df)
        df = df.withColumnRenamed('anomalyScore', self.score_column)
        return df

    def _get_params_path(self, path):
        return os.path.join(path, 'params.json')

    def _get_iforest_path(self, path):
        return os.path.join(path, 'iforest')

    def _get_scaler_path(self, path):
        return os.path.join(path, 'scaler')

    def _get_index_path(self, path, feature):
        return os.path.join(path, 'indexes', feature)

    def save(self, path, spark_session=None):
        file_manager = FileManager(path, spark_session)
        file_manager.save_to_file(self.get_params(), self._get_params_path(path), format='json')
        self.iforest_model.write().overwrite().save(self._get_iforest_path(path))
        self.scaler_model.write().overwrite().save(self._get_scaler_path(path))

        if len(self.categorical_features):
            for feature, index in zip(self.categorical_features, self.indexes):
                index.write().overwrite().save(self._get_index_path(path, feature))

    def load(self, path, spark_session=None):
        self.iforest_model = IForestModel.load(self._get_iforest_path(path))
        self.scaler_model = StandardScalerModel.load(self._get_scaler_path(path))

        file_manager = FileManager(path, spark_session)
        params = file_manager.load_from_file(self._get_params_path(path), format='json')
        self.set_params(**params)

        self.indexes = []
        for feature in self.categorical_features:
            self.indexes.append(StringIndexerModel.load(self._get_index_path(path, feature)))
