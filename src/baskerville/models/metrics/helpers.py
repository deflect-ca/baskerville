# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from functools import wraps
# Accumulators
from baskerville.spark.helpers import DictAccumulatorParam

CLIENT_PREDICTION_ACCUMULATOR = None
CLIENT_REQUEST_SET_COUNT = None


def has_self(fn):
    return hasattr(fn, '__self__')


def set_accumulators(spark):
    global CLIENT_PREDICTION_ACCUMULATOR, CLIENT_REQUEST_SET_COUNT

    from collections import defaultdict
    CLIENT_PREDICTION_ACCUMULATOR = spark.sparkContext.accumulator(
        defaultdict(float), DictAccumulatorParam(defaultdict(float))
    )
    CLIENT_REQUEST_SET_COUNT = spark.sparkContext.accumulator(
        defaultdict(int), DictAccumulatorParam(defaultdict(int))
    )


def is_wrapped_method(fn):
    """
    Checks if fn is a wrapped method/ function
    :param func fn:
    :return:
    """
    import inspect
    return '__wrapped__' in fn.__dict__ and \
           inspect.ismethod(fn.__dict__['__wrapped__'])


def set__self__(fn):
    """
    Sets the __self__ attribute if it is lost in wrapping
    :param func fn:
    :return:
    """
    @wraps(fn)
    def wrapped_f(self, func_to_watch_for, *args, **kwargs):
        if not has_self(func_to_watch_for):
            if not is_wrapped_method(func_to_watch_for):
                raise ValueError(
                    f'{func_to_watch_for.__name__} is not a bound method -'
                    f'currently unsupported.'
                )
            func_to_watch_for.__self__ = func_to_watch_for.__dict__[
                '__wrapped__'].__self__
        return fn(self, func_to_watch_for, *args, **kwargs)

    return wrapped_f


def update_avg_hosts_counter(metric, self, result):
    """
    Averages the host predictions and increments the metric by labels
    :param metric:
    :param self:
    :return:
    """
    global CLIENT_PREDICTION_ACCUMULATOR, CLIENT_REQUEST_SET_COUNT
    if self._can_predict:
        v1 = CLIENT_PREDICTION_ACCUMULATOR.value
        v2 = CLIENT_REQUEST_SET_COUNT.value
        for k, v in v2.items():
            v1[k] = v1[k] / v
            metric.labels(k).set(v1[k])


def incr_counter_for_logs_df(metric, self, result):
    """
    Increment by the number of requests / request sets
    :param metric:
    :param SparkPipelineBase self:
    :return:
    """
    metric.inc(self.logs_df.count())


def set_counter_for_logs_df(metric, self, result):
    """
    Increment by the number of requests / request sets
    :param metric:
    :param SparkPipelineBase self:
    :return:
    """
    metric.set(self.logs_df.count())


def set_gauge_for_request_set_cache(metric, self):
    """
    Sets the value of the metric to the current length of the request sets
    cache - short term cache
    :param metric:
    :param self:
    :return:
    """
    metric.set(len(self.request_set_cache))


def set_gauge_for_request_set_persistent_cache(metric, self):
    """
    Sets the value of the metric to the current length of the request sets
    cache - short term cache
    :param metric:
    :param self:
    :return:
    """
    if self.request_set_cache.persistent_cache:
        metric.set(self.request_set_cache.persistent_cache.count())


def increment_metric(metric, self=None):  # noqa
    metric.inc()


def set_attack_score(metric, self, result):
    """
    For every target, it sets the precalculated attack score
    """
    if not self.collected_df_target_score:
        return
    for row in self.collected_df_target_score:
        metric.labels(target=row.target).set(row.attack_score)


def set_attack_prediction(metric, self, result):
    """
    For every target, it sets the precalculated attack prediction
    """
    if not self.collected_df_attack:
        return
    for row in self.collected_df_attack:
        metric.labels(target=row.target).set(row.attack_prediction)


def set_attack_threshold(metric, self, result):
    metric.labels(value='attack_threshold').set(self.config.engine.attack_threshold)


def set_total_rs_count(metric, self, result):
    if not self.collected_df_attack:
        return
    for row in self.collected_df_attack:
        metric.labels(target=row.target).set(row.total)


def set_ip_prediction_count(metric, self, result):
    """
    For every target, it sets the regular and the anomaly counts
    """
    if not self.collected_df_target_score:
        return

    for row in self.collected_df_attack:
        metric.labels(
            target=row.target, kind='regular'
        ).set(row.regular)
        metric.labels(
            target=row.target, kind='anomaly'
        ).set(row.anomaly)
        metric.labels(
            target=row.target, kind='attack'
        ).set(row.attack)
