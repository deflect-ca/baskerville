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
from pyspark.sql import functions as F


class AnomalyModel(ModelInterface):

    def __init__(self, feature_map_column='features',
                 features=None,
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
        self.feature_map_column = feature_map_column
        self.storage_level = storage_level

        self.scaler_model = None
        self.iforest_model = None
        self.threshold = threshold
        self.indexes = None
        self.features_vector = 'features_values'
        self.features_vector_scaled = 'features_values_scaled'
        self.prefix_feature = 'cf_'
        self.prefix_index = 'cf_index_'

    def _create_regular_features_vector(self, df):
        if not isinstance(self.features, dict):
            raise RuntimeError('AnomalyModel expects a dictionary of features '
                               '{feature1: {categorical: True, string=True}, '
                               ' feature2: {categorical: False}}')
        res = map_to_array(
            df,
            map_col=self.feature_map_column,
            array_col=self.features_vector,
            map_keys=[k for k, v in self.features.items() if not v['categorical']]
        ).persist(StorageLevelFactory.get_storage_level(self.storage_level))
        df.unpersist()

        return res.withColumn(
            self.features_vector,
            udf_to_dense_vector(self.features_vector)
        )

    def categorical_features(self):
        categorical_features = []
        for feature, v in self.features.items():
            if 'categorical' not in v or not v['categorical']:
                continue
            categorical_features.append(feature)
        return categorical_features

    def categorical_string_features(self):
        categorical_features = []
        for feature, v in self.features.items():
            if 'categorical' not in v or not v['categorical']:
                continue
            if 'string' not in v or not v['string']:
                continue
            categorical_features.append(feature)
        return categorical_features

    def _create_feature_columns(self, df):
        for feature in self.categorical_features():
            df = df.withColumn(f'{self.prefix_feature}{feature}', F.col(f'{self.feature_map_column}.{feature}'))
        return df

    def _drop_feature_columns(self, df):
        return df.drop(*[f'{self.prefix_feature}{feature}' for feature in self.categorical_features()])

    def _create_indexes(self, df):
        self.indexes = {}
        for feature in self.categorical_string_features():
            indexer = StringIndexer(inputCol=f'{self.prefix_feature}{feature}',
                                    outputCol=f'{self.prefix_index}{feature}') \
                .setHandleInvalid('keep') \
                .setStringOrderType('alphabetAsc')
            self.indexes[feature] = indexer.fit(df)

    def _add_categorical_features(self, df, feature_column):
        columns = []
        for feature in self.categorical_features():
            if feature in self.indexes:
                indexer = self.indexes[feature]
                df = indexer.transform(df)
                if indexer.getOutputCol() not in df.columns:
                    df = df.withColumn(indexer.getOutputCol(), F.lit(None))
                columns.append(indexer.getOutputCol())
            else:
                columns.append(f'{self.prefix_feature}{feature}')

        df = df.withColumn('features_all', udf_add_to_dense_vector(
            feature_column, array(*index_columns))) \
            .drop(*index_columns, feature_column) \
            .withColumnRenamed('features_all', feature_column)
        return df

    def train(self, df):
        df = self._create_regular_features_vector(df)

        scaler = StandardScaler()
        scaler.setInputCol(self.features_vector)
        scaler.setOutputCol(self.features_vector_scaled)
        scaler.setWithMean(self.scaler_with_mean)
        scaler.setWithStd(self.scaler_with_std)
        self.scaler_model = scaler.fit(df)
        df = self.scaler_model.transform(df).persist(
            StorageLevelFactory.get_storage_level(self.storage_level)
        )
        df = df.drop(self.features_vector)

        df = self._create_feature_columns(df)
        self._create_indexes(df)
        df = self._add_categorical_features(df, self.features_vector_scaled)
        df = self._drop_feature_columns(df)

        iforest = IForest(
            featuresCol=self.features_vector_scaled,
            predictionCol=self.prediction_column,
            # anomalyScore=self.score_column,
            numTrees=self.num_trees,
            maxSamples=self.max_samples,
            maxFeatures=self.max_features,
            maxDepth=self.max_depth,
            contamination=self.contamination,
            bootstrap=self.bootstrap,
            approxQuantileRelativeError=self.approximate_quantile_relative_error,
            numCategoricalFeatures=len(self.categorical_features())
        )
        iforest.setSeed(self.seed)
        params = {'threshold': self.threshold}
        self.iforest_model = iforest.fit(df, params)

        df = df.drop(self.features_vector_scaled)
        df.unpersist()

    def predict(self, df):
        df = self._create_regular_features_vector(df)
        df = self.scaler_model.transform(df)
        if len(self.categorical_features):
            df = self._add_categorical_features(
                df, self.features_values_scaled
            )
        df = self.iforest_model.transform(df)
        df = df.withColumnRenamed('anomalyScore', self.score_column)
        df = df.drop(self.features_vector_scaled)
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

        for feature, index in self.indexes.items():
            index.write().overwrite().save(self._get_index_path(path, feature))

    def load(self, path, spark_session=None):
        self.iforest_model = IForestModel.load(self._get_iforest_path(path))
        self.scaler_model = StandardScalerModel.load(self._get_scaler_path(path))

        file_manager = FileManager(path, spark_session)
        params = file_manager.load_from_file(self._get_params_path(path), format='json')
        self.set_params(**params)

        self.indexes = {}
        for feature in self.categorical_string_features():
            self.indexes[feature] = StringIndexerModel.load(self._get_index_path(path, feature))
