from typing import Optional, List

import dateutil
from pyspark.ml import Estimator
from pyspark.ml.evaluation import BinaryClassificationEvaluator, ParamMap, \
    Evaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, \
    CrossValidatorModel
from pyspark.sql import functions as F

from baskerville.db.models import RequestSet
from baskerville.models.anomaly_model import AnomalyModel
from baskerville.models.config import BaskervilleConfig
from baskerville.spark import get_or_create_spark_session
from baskerville.spark.helpers import load_df_from_table
from baskerville.util.enums import LabelEnum
from baskerville.util.helpers import parse_config


# # https://stackoverflow.com/questions/52847408/pyspark-extract-roc-curve
# # Scala version implements .roc() and .pr()
# # Python: https://spark.apache.org/docs/latest/api/python/_modules/pyspark/mllib/common.html
# # Scala: https://spark.apache.org/docs/latest/api/java/org/apache/spark/mllib/evaluation/BinaryClassificationMetrics.html
# class CurveMetrics(BinaryClassificationMetrics):
#     def __init__(self, *args):
#         super(CurveMetrics, self).__init__(*args)
#
#     def _to_list(self, rdd):
#         points = []
#         # Note this collect could be inefficient for large datasets
#         # considering there may be one probability per datapoint (at most)
#         # The Scala version takes a numBins parameter,
#         # but it doesn't seem possible to pass this from Python to Java
#         for row in rdd.collect():
#             # Results are returned as type scala.Tuple2,
#             # which doesn't appear to have a py4j mapping
#             points += [(float(row._1()), float(row._2()))]
#         return points
#
#     def get_curve(self, method):
#         rdd = getattr(self._java_model, method)().toJavaRDD()
#         return self._to_list(rdd)
def _parallelFitTasks(est, train, eva, validation, epm, collectSubModel):
    """
    Creates a list of callables which can be called from different threads to fit and evaluate
    an estimator in parallel. Each callable returns an `(index, metric)` pair.

    :param est: Estimator, the estimator to be fit.
    :param train: DataFrame, training data set, used for fitting.
    :param eva: Evaluator, used to compute `metric`
    :param validation: DataFrame, validation data set, used for evaluation.
    :param epm: Sequence of ParamMap, params maps to be used during fitting & evaluation.
    :param collectSubModel: Whether to collect sub model.
    :return: (int, float, subModel), an index into `epm` and the associated metric value.
    """
    modelIter = est.fitMultiple(train, epm)

    def singleTask():
        index, model = next(modelIter)
        # todo: we need ROC here:
        metric = eva.evaluate(model.transform(validation, epm[index]))
        return index, metric, model if collectSubModel else None

    return [singleTask] * len(epm)


class IForestCrossValidator(CrossValidator):
    def __init__(self, estimator: Optional[Estimator] = ...,
                 estimatorParamMaps: Optional[List[ParamMap]] = ...,
                 evaluator: Optional[Evaluator] = ...,
                 numFolds: int = ...,
                 seed: Optional[int] = ...,
                 parallelism: int = ...,
                 collectSubModels: bool = ...):
        super().__init__(estimator, estimatorParamMaps, evaluator, numFolds,
                         seed, parallelism, collectSubModels)
        self.start = None
        self.stop = None
        self.db_config = None
        self.attacks_df = None

    def set_start(self, start):
        self.start = start

    def set_stop(self, stop):
        self.stop = stop

    def set_db_config(self, db_config):
        self.db_config = db_config

    def get_attacks_df(self):
        self.attacks_df = get_attack_df(start, stop, db_config=self.db_config)

    def get_train(self, df, condition):
        """
        Train should not have anomalies, so no known attacks should be included
        """
        return df.withColumn(
            'label', F.lit(LabelEnum.benign)
        ).filter(~condition).cache()

    def get_validation(self, df, condition):
        """
        Validation should contain anomalies / attacks
        """
        df = df.filter(condition).cache()
        df = df.join(
            self.attacks_df,
            df.ip == self.attacks_df.ip_attacker,
            how='left'
        )
        return df.withColumn(
            'label', F.when(
                F.col('ip_attacker').isNull(), LabelEnum.benign
            ).otherwise(LabelEnum.malicious)
        ).cache()

    def _fit(self, dataset):
        import numpy as np

        est = self.getOrDefault(self.estimator)
        epm = self.getOrDefault(self.estimatorParamMaps)
        numModels = len(epm)
        eva = self.getOrDefault(self.evaluator)
        nFolds = self.getOrDefault(self.numFolds)
        seed = self.getOrDefault(self.seed)
        h = 1.0 / nFolds
        randCol = self.uid + "_rand"
        df = dataset.select("*", F.rand(seed).alias(randCol))
        metrics = [0.0] * numModels

        from multiprocessing.pool import ThreadPool
        pool = ThreadPool(processes=min(self.getParallelism(), numModels))
        subModels = None
        collectSubModelsParam = self.getCollectSubModels()
        if collectSubModelsParam:
            subModels = [[None for j in range(numModels)] for i in
                         range(nFolds)]

        for i in range(nFolds):
            validateLB = i * h
            validateUB = (i + 1) * h
            condition = (df[randCol] >= validateLB) & (
                        df[randCol] < validateUB)
            validation = self.get_validation(df, condition)
            train = self.get_train(df, condition)

            tasks = _parallelFitTasks(est, train, eva, validation, epm,
                                      collectSubModelsParam)
            for j, metric, subModel in pool.imap_unordered(lambda f: f(),
                                                           tasks):
                metrics[j] += (metric / nFolds)
                if collectSubModelsParam:
                    subModels[i][j] = subModel

            validation.unpersist()
            train.unpersist()

        if eva.isLargerBetter():
            bestIndex = np.argmax(metrics)
        else:
            bestIndex = np.argmin(metrics)
        bestModel = est.fit(dataset, epm[bestIndex])
        return self._copyValues(
            CrossValidatorModel(bestModel, metrics, subModels))


def get_train_df(start, stop, db_config):
    # todo: date might have to be str
    return load_df_from_table(
        RequestSet.__tablename__,
        db_config,
        (F.col('stop') >= start & F.col('stop') <= stop)
    ).cache()


def get_test_df(start, stop, db_config):
    """
    The test set should be comprised of part of attack data and part of
    normal data
    """
    attack_df = get_attack_df(start, stop, db_config)

    # todo: date might have to be str
    normal_data = load_df_from_table(
        RequestSet.__tablename__,
        db_config,
        (F.col('stop') >= start & F.col('stop') <= stop)
    ).cache()
    normal_data = normal_data.join(
        attack_df, normal_data.ip == attack_df.ip_attacker, how='left'
    )
    test_df = normal_data.withColumn(
        'label', F.when(F.col('ip_attacker').isNull(), 0.0).otherwise(1.0)
    ).cache()
    return test_df


def get_attack_df(start, stop, db_config, id_attack=None):
    q = f'(SELECT b.id_attack, a.value as ip_attacker FROM public.attributes as a ' \
        f'INNER JOIN attribute_attack_link as b ON ' \
        f'attribute_attack_link.id_attribute =attributes.id ' \
        f'where attribute_attack_link.id_attack IS IN ' \
        f'(SELECT id from attacks where stop >= \'{start}\' ' \
        f'and stop <= \'{stop}\')) as attack_ips;'
    return load_df_from_table(
        q,
        db_config,
        None
    ).cache()


def cross_validate(start, stop, config: BaskervilleConfig, num_folds=3):
    _ = get_or_create_spark_session(config.spark)
    # anomaly_model = load_anomaly_model(model_path)
    # feature_names = anomaly_model.features.keys()
    evaluator = BinaryClassificationEvaluator()
    param_grid = ParamGridBuilder()

    evaluation_df = get_train_df(start, stop, db_config={})
    test_df = get_train_df(start, stop, db_config={})

    for k, v in config.engine.training.cv_params:
        param_grid.addGrid(k, v)

    param_grid = param_grid.build()
    cross_validator = IForestCrossValidator(
        estimator=AnomalyModel,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=num_folds
    )

    cv_model = cross_validator.fit(evaluation_df)

    # cv_model uses the best model found
    prediction = cv_model.transform(test_df)
    selected = prediction.select("id", "anomalyScore", "prediction")
    for row in selected.collect():
        print(row)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-n', '--numfolds',
        help='The number of cross-validation folds, defaults to 3',
        default=3
    )
    parser.add_argument(
        '-c', '--config', help='The path to config.yaml'
    )
    parser.add_argument(
        '-s', '--start', help='Date used to filter stop field',
        type=dateutil.parser.parse
    )
    parser.add_argument(
        '-u', '--stop', help='Date used to filter stop field',
        type=dateutil.parser.parse
    )

    args = parser.parse_args()
    num_folds = args.numfolds
    config_path = args.config
    start = args.start
    stop = args.stop
    config = parse_config(config_path)
    bask_config = BaskervilleConfig(config).validate()
    cross_validate(start, stop, bask_config, num_folds=num_folds)


