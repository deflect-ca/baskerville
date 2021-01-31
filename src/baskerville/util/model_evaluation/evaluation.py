import dateutil
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, \
    CrossValidatorModel
from pyspark.sql import functions as F

from baskerville.db import set_up_db
from baskerville.db.models import RequestSet, Attack, Attribute
from baskerville.models.anomaly_model import AnomalyModel
from baskerville.spark.helpers import load_df_from_table
from baskerville.util.model_interpretation.helpers import \
    get_spark_session_with_iforest, load_anomaly_model


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
        metric = eva.evaluate(model.transform(validation, epm[index]))
        return index, metric, model if collectSubModel else None

    return [singleTask] * len(epm)


class IForestCrossValidator(CrossValidator):
    def get_train(self, df, condition):
        return df.filter(~condition).cache()

    def get_validation(self, df, condition):
        return df.filter(condition).cache()

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


def cross_validate(model_path, train_df, test_df, params, num_folds=3):
    _ = get_spark_session_with_iforest()
    anomaly_model = load_anomaly_model(model_path)
    # feature_names = anomaly_model.features.keys()
    evaluator = BinaryClassificationEvaluator()
    param_grid = ParamGridBuilder()

    for k, v in params:
        param_grid.addGrid(k, v)

    param_grid = param_grid.build()
    cross_validator = IForestCrossValidator(estimator=AnomalyModel,
                              estimatorParamMaps=param_grid,
                              evaluator=evaluator,
                              numFolds=num_folds)

    cv_model = cross_validator.fit(train_df)

    # cv_model uses the best model found
    prediction = cv_model.transform(test_df)
    selected = prediction.select("id", "text", "probability", "prediction")
    for row in selected.collect():
        print(row)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p', '--datapath', help='The path to the data'
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
    data_path = args.datapath
    start = args.start
    stop = args.stop

    cross_validate()


