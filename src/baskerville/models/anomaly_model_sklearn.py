# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.model_interface import ModelInterface
from baskerville.spark import get_spark_session
import os

from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
import pandas as pd
from typing import NamedTuple
from pyspark.sql import functions as F
from pyspark.sql import types as T

from baskerville.util.file_manager import FileManager

prediction_schema = T.StructType([
    T.StructField("prediction", T.FloatType(), False),
    T.StructField("score", T.FloatType(), True)
])


class AnomalyDetector(NamedTuple):
    model: any
    features: list
    scaler: any
    threshold: float
    features_col: str = 'features'
    prediction_col: str = 'prediction'
    score_col: str = 'score'


def extract_features_in_order(feature_dict, model_features):
    """
    Returns the model features in the order the model requires them.
    """
    return [feature_dict[feature] for feature in model_features]


class AnomalyModelSklearn(ModelInterface):

    def __init__(self, feature_map_column='features', features=None, prediction_column="prediction",
                 score_column="score",
                 num_trees=100, max_samples="auto", max_features=1.0, n_jobs=1, verbose=10,
                 contamination=0.1, bootstrap=False,
                 seed=777,
                 scaler_with_mean=False, scaler_with_std=True):
        super().__init__()
        self.prediction_column = prediction_column
        self.score_column = score_column
        self.num_trees = num_trees
        self.max_samples = max_samples
        self.max_features = max_features
        self.contamination = contamination
        self.bootstrap = bootstrap
        self.seed = seed
        self.n_jobs = n_jobs
        self.verbose = verbose

        self.scaler_with_mean = scaler_with_mean
        self.scaler_with_std = scaler_with_std

        self.scaler_model = None
        self.iforest_model = None

        self.features = features
        self.feature_map_column = feature_map_column
        self.anomaly_detector_broadcast = None

    def train(self, df):
        df = df.toPandas()
        features = self.features
        if not features or len(features) == 0:
            self.features = [*df[self.feature_map_column][0].keys()]

        df[self.features] = df[self.feature_map_column].apply(pd.Series)
        df.drop(self.feature_map_column, axis=1, inplace=True)

        self.scaler_model = StandardScaler(
            with_mean=self.scaler_with_mean, with_std=self.scaler_with_std)
        x_train = self.scaler_model.fit_transform(
            df[self.features].values
        )

        self.iforest_model = IsolationForest(
            n_estimators=self.num_trees,
            max_samples=self.max_samples,
            contamination=self.contamination,
            max_features=self.max_features,
            bootstrap=self.bootstrap,
            n_jobs=self.n_jobs,
            random_state=self.seed,
            verbose=self.verbose
        )
        self.iforest_model.fit(x_train)

    def predict(self, df):
        from baskerville.models.metrics.helpers import CLIENT_PREDICTION_ACCUMULATOR
        global CPA, CRSC

        CPA = CLIENT_PREDICTION_ACCUMULATOR
        CRSC = CLIENT_PREDICTION_ACCUMULATOR

        def predict_dict(target, dict_features, update_metrics=False):
            """
            Scale the feature values and use the model to predict
            :param dict[str, float] dict_features: the feature dictionary
            :param update_metrics:
            :return: 0 if normal, 1 if abnormal, -1 if something went wrong
            """
            global CPA, CRSC
            import json
            prediction = 0, 0.
            score = None

            if isinstance(dict_features, str):
                dict_features = json.loads(dict_features)
            try:
                x = dict_features
                detector = self.anomaly_detector_broadcast.value
                x = [extract_features_in_order(x, detector.features)]
                x = detector.scaler.transform(x)
                y = 0.5 - detector.model.decision_function(x)
                threshold = detector.threshold if detector.threshold > 0 \
                    else 0.5 - detector.model.threshold_
                prediction = float((y > threshold)[0])
                score = float(y[0])
                # because of net.razorvine.pickle.PickleException: expected
                # zero arguments for construction of ClassDict
                # (for numpy.core.multiarray._reconstruct):

            except ValueError:
                import traceback
                traceback.print_exc()
                print('Cannot predict:', dict_features)

            if update_metrics:
                CRSC += {target: 1}
                CPA += {target: prediction}

            return prediction, score

        udf_predict_dict = F.udf(predict_dict, prediction_schema)

        df = df.withColumn(
            'y',
            udf_predict_dict(
                'target',
                'features',
                F.lit(False)  # todo
            )
        )

        df = df.withColumn(
            self.prediction_column, F.col('y.prediction')
        ).withColumn(
            self.score_column, F.col('y.score')
        ).drop('y')
        return df

    def save(self, path, spark_session=None):
        file_manager = FileManager(path, spark_session)
        file_manager.save_to_file(self.get_params(), os.path.join(
            path, 'params.json'), format='json')
        file_manager.save_to_file(self.iforest_model, os.path.join(
            path, 'iforest.pickle'), format='pickle')
        file_manager.save_to_file(self.scaler_model, os.path.join(
            path, 'scaler.pickle'), format='pickle')

    def load(self, path, spark_session=None):
        file_manager = FileManager(path, spark_session)
        params = file_manager.load_from_file(
            os.path.join(path, 'params.json'), format='json')
        self.set_params(**params)
        self.iforest_model = file_manager.load_from_file(
            os.path.join(path, 'iforest.pickle'), format='pickle')
        self.scaler_model = file_manager.load_from_file(
            os.path.join(path, 'scaler.pickle'), format='pickle')

        anomaly_detector = AnomalyDetector(
            model=self.iforest_model,
            features=self.features,
            scaler=self.scaler_model,
            threshold=0.0
        )

        self.anomaly_detector_broadcast = get_spark_session().sparkContext.broadcast(
            anomaly_detector
        )
