import abc
from collections import defaultdict
from typing import NamedTuple
import os

import _pickle as cPickle

from baskerville.util.helpers import get_classifier_load_path, \
    get_scaler_load_path, class_from_str
from pyspark.ml.feature import StandardScalerModel
from pyspark_iforest.ml.iforest import IForestModel

from baskerville.features.helpers import extract_features_in_order
from baskerville.spark import get_spark_session
from baskerville.spark.helpers import load_model_from_path
from baskerville.spark.schemas import prediction_schema
from pyspark.sql import functions as F


class AnomalyDetector(NamedTuple):
    model: any
    features: list
    host_encoder: any
    scaler: any
    threshold: float
    features_col: str = 'features'
    prediction_col: str = 'prediction'
    score_col: str = 'score'


class AnomalyDetectorManagerBase(object, metaclass=abc.ABCMeta):
    """
    todo: might raise memory consumption a bit
    V
    """
    anomaly_detector = None
    anomaly_detector_bc = None

    @abc.abstractmethod
    def predict(self, df):
        pass

    def load_classifier(self, classifier, algorithm):
        return classifier

    def load_scaler(self, scaler, scaler_type):
        return scaler

    def load_features(self, features):
        return features

    def load_threshold(self, threshold):
        return threshold

    def load_host_encoder(self, host_encoder):
        return host_encoder

    @abc.abstractmethod
    def load(self, model):
        """
        Instantiates an AnomalyDetector object, using the
        baskerville.db.models.Model
        :param baskerville.db.models.Model model:
        :param register_metrics:
        :return:
        """
        self.anomaly_detector = AnomalyDetector(
            model=self.load_classifier(model.classifier, model.algorithm),
            features=self.load_features(model.features),
            scaler=self.load_scaler(model.scaler, model.scaler_type),
            threshold=self.load_threshold(model.threshold),
            host_encoder=self.load_host_encoder(model.host_encoder),
        )

    def broadcast(self):
        self.anomaly_detector_bc = get_spark_session().sparkContext.broadcast(
            self.anomaly_detector
        )


class ScikitAnomalyDetectorManager(AnomalyDetectorManagerBase):

    def predict(self, df):
        from baskerville.models.metrics.helpers import \
            CLIENT_PREDICTION_ACCUMULATOR, CLIENT_REQUEST_SET_COUNT
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
            threshold = None
            score = None

            if isinstance(dict_features, str):
                dict_features = json.loads(dict_features)
            try:
                x = dict_features
                detector = self.anomaly_detector_bc.value
                x = [extract_features_in_order(x, detector.features)]
                x = detector.scaler.transform(x)
                y = 0.5 - detector.model.decision_function(x)
                threshold = detector.threshold if detector.threshold \
                    else 0.5 - detector.model.threshold_
                prediction = float((y > threshold)[0])
                score = float(y[0])
                # because of net.razorvine.pickle.PickleException: expected
                # zero arguments for construction of ClassDict
                # (for numpy.core.multiarray._reconstruct):
                threshold = float(threshold)

            except ValueError:
                import traceback
                traceback.print_exc()
                print('Cannot predict:', dict_features)

            if update_metrics:
                CRSC += {target: 1}
                CPA += {target: prediction}

            return prediction, score, threshold

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
            self.anomaly_detector.prediction_col, F.col('y.prediction')
        ).withColumn(
            self.anomaly_detector.score_col, F.col('y.score')
        ).withColumn(
            'threshold', F.col('y.threshold')
        ).drop('y')
        return df

    def load_classifier(self, classifier, algorithm):
        return cPickle.loads(classifier)

    def load_scaler(self, scaler, scaler_type):
        return cPickle.loads(scaler) if scaler else scaler

    def load_host_encoder(self, host_encoder):
        return cPickle.loads(host_encoder) if host_encoder else host_encoder

    def load(self, model):
        super().load(model)
        self.broadcast()


class SparkAnomalyDetectorManager(AnomalyDetectorManagerBase):

    def predict(self, df):
        from baskerville.spark.udfs import to_dense_vector_udf

        df = df.withColumn(
            'vectorized_features',
            to_dense_vector_udf('vectorized_features')
        )

        df = self.anomaly_detector.scaler.transform(df)
        df = self.anomaly_detector.model.transform(df)
        df = df.withColumnRenamed('anomalyScore', 'score')
        return df

    def load_classifier(self, classifier, algorithm):
        return load_model_from_path(
            f'{algorithm}Model',
            bytes.decode(classifier, 'utf8')
        )

    def load_scaler(self, scaler, scaler_type):
        if scaler:
            return load_model_from_path(
                f'{scaler_type}Model',
                bytes.decode(scaler, 'utf8')
            )
        return scaler

    def load_host_encoder(self, host_encoder):
        """
        # todo
        :param host_encoder:
        :return:
        """
        return host_encoder

    def load(self, model):
        model_path = bytes.decode(model.classifier, 'utf8')
        self.anomaly_detector = AnomalyDetector(
            model=class_from_str(f'{model.algorithm}Model').load(
                get_classifier_load_path(model_path)
            ),
            features=model.features,
            scaler=class_from_str(f'{model.scaler_type}Model').load(
                get_scaler_load_path(model_path)
            ),
            threshold=self.load_threshold(model.threshold),
            host_encoder=None,
        )
