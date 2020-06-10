# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from functools import wraps

from attr import dataclass
from baskerville.models.metrics.helpers import set__self__
from baskerville.util.enums import MetricClassEnum, MetricTypeEnum
from prometheus_client import Counter, Gauge, Summary, Histogram, Info, Enum, \
    Metric

from baskerville.util.helpers import Singleton


@dataclass
class StatsHook(object):
    metric: Metric
    wrapped_func_name: str
    hooks_to_callbacks: dict


class MetricsRegistry(Singleton):
    """
    A Singleton class that keeps a registry with metrics of the following
    types:
    - Counter
    - Gauge
    - Summary
    - Histogram
    - Info
    - Enum

    more info: https://github.com/prometheus/client_python

    Provides the following wrappers:
    - `before_wrapper`
    - `after_wrapper`
    - `with_wrapper`: used for timing methods/ functions and tracking with a
    Summary / Histogram metric type
    if they are in progress or not, either with a Gauge
    or . Wraps the method/ function to
    be timed and

    """
    _prefix = 'baskerville_'
    __registry = {}
    __metric_classes = {
        MetricClassEnum.counter: Counter,
        MetricClassEnum.gauge: Gauge,
        MetricClassEnum.summary: Summary,
        MetricClassEnum.histogram: Histogram,
        MetricClassEnum.info: Info,
        MetricClassEnum.enum: Enum,
    }
    __func_name_to_state = {}

    def __len__(self):
        return len(self.registry)

    def prefix(self, name):
        return f'{self._prefix}{name}'

    @property
    def registry(self):
        return self.__registry

    @staticmethod
    def get_parent_name(fn):
        """
        Returns the class's name if any, else emtpy.
        :param callable fn: the callable to get the class's name for.
        :return: the class's name if a bound method else an empty string
        :rtype: str
        """
        return fn.__self__.__class__.__name__ \
            if hasattr(fn, '__self__') else ''

    def get_name(self, fn=None, name=None):
        """
        Returns the metric name
        :param name:
        :param callable fn:
        :return:
        :rtype:str
        """
        prefix = self._prefix

        if name:
            return self.prefix(name)
        if callable(fn):
            return f'{prefix}' \
                   f'{fn.__self__.__class__.__name__.lower()}_' \
                   f'{fn.__name__}' if hasattr(fn, '__self__') \
                else self.prefix(fn.__name__)
        raise ValueError('Either a function or a name should be provided')

    @staticmethod
    def get_description(fn, parent_name):
        return f'Time Performance of {parent_name} "{fn.__name__}"'

    def name_integrity_check(self, fn, metric_name=None):
        """
        Checks whether the name that corresponds to the fn is already in the
        registry or not.
        :param callable fn:
        :param str metric_name:
        :return:
        """
        name = self.get_name(fn, metric_name)
        if name in self.registry:
            raise ValueError(f'Duplicate name: {name}')
        return name

    def before_wrapper(self, fn, metric_name):
        """

        :param callable fn:
        :param str metric_name:
        :return:
        """
        @wraps(fn)
        def wrapper_f(*args, **kwargs):
            stats_h = self.registry[metric_name]
            stats_h.hooks_to_callbacks['before'](stats_h.metric, fn.__self__)
            return fn(*args, **kwargs)

        # restore __self__ because wrapping loses it.
        wrapper_f.__self__ = fn.__self__

        return wrapper_f

    def after_wrapper(self, fn, metric_name):
        """
        Wraps func and executes the stats hook after func has finished
        executing
        :param callable fn: the function to be executed
        :param str metric_name: the metric name
        :return: the wrapped fuction
        """

        @wraps(fn)
        def wrapper_f(*args, **kwargs):
            result = fn(*args, **kwargs)
            stats_h = self.registry[metric_name]
            stats_h.hooks_to_callbacks['after'](stats_h.metric, fn.__self__)
            return result

        # restore __self__ because wrapping loses it.
        wrapper_f.__self__ = fn.__self__

        return wrapper_f

    def with_wrapper(
            self, fn, metric_name, kind=MetricTypeEnum.progress, method='time'
    ):
        """
        Wraps a function and uses a metric already stored in registry that
        supports with usage (context manager) to time the execution of fn
        :param func fn: the function to be timed
        :param str metric_name: the metric name
        :param MetricTypeEnum kind: the metric type
        :param str method: which method to call on the metric, default 'time'
        :return: the wrapped function
        """

        @wraps(fn)
        def wrapper_f(*args, **kwargs):
            metric = self.__registry[metric_name]
            if kind == MetricTypeEnum.progress and isinstance(metric, Gauge):
                metric.set_to_current_time()

            with getattr(metric, method)():
                return fn(*args, **kwargs)

        return wrapper_f

    def state_wrapper(self, func, metric_name):
        @wraps(func)
        def wrapper_f(*args, **kwargs):
            # set state before calling the function
            metric = self.__registry[metric_name]
            metric.state(self.__func_name_to_state[func.__name__])
            return func(*args, **kwargs)

        return wrapper_f

    def register_info(self, metric_name, description, data):
        """
        Registers an Info type of metric, e.g. the current version
        :param str metric_name: the metric name
        :param str description: details about what it measures
        :param T data: what to display
        :return:
        """
        if metric_name in self.registry:
            raise ValueError(f'Duplicate name: {metric_name}')
        self.registry[metric_name] = Info(metric_name, description)
        self.registry[metric_name].info(data)

    def register_states(self, metric_name, description, states_to_methods):
        """
        Registers an enum metric to register states (boolean)
        :param str metric_name: the name of the state metric
        :param str description: what does the state metric measure
        :param dict[str, T] states_to_methods: key value pairs of the available
        states and the respective methods
        :return:
        """
        self.__registry[metric_name] = Enum(
            metric_name,
            description,
            states=list([str(k)for k in states_to_methods.keys()])
        )

        updated_state_to_method = {}

        for state, method in states_to_methods.items():
            self.__func_name_to_state[method.__name__] = str(state)
            updated_state_to_method[state] = self.state_wrapper(
                method, metric_name)

        return updated_state_to_method

    def register_timer(self, metric_name, func, metric_description=None):
        """
        Registers a Summary metric that supports timing the execution fo
        :param metric_name:
        :param func:
        :return:
        """
        parent_name = self.get_parent_name(func)
        name = self.name_integrity_check(func, metric_name)

        summary = Summary(
            name,
            metric_description or self.get_description(func, parent_name)
        )

        self.registry[name] = summary
        wrapper_f = self.with_wrapper(func, name)

        return wrapper_f

    @set__self__
    def register_action_hook(
            self,
            func_to_watch_for,
            callback,
            when='after',
            metric_name=None,
            metric_description=None,
            metric_cls=MetricClassEnum.counter,
            labelnames=None,
    ):
        """
        Registers an action for the func_to_watch_for, depending on `when`.
        This means that the func_to_watch_for is wrapped so that the callback
        that updates the metric is executed before or after the
        func_to_watch_for has been called.
        :param callable func_to_watch_for: the function that acts as a trigger for
        the metric to be updated
        :param callable callback: how to update the metric. It could be a simple
        :param metric_description: a short description of the metric
        lambda expression that takes as input two arguments, metric and self
        and updates the metric: metric.inc(). Note: the callback signature must
        have two arguments, the first one is the metric itself and the second
        one is the `self` of the wrapped method `func_to_watch_for`
        (only methods are currently supported)
        :param str when: `before` or `after`, it defines when should the
        callback be executed.
        :param str metric_name: the name of the metric
        :param MetricClassEnum metric_cls: the type of metric
        :param list[str] labelnames:a list with the labelnames if any. None by
        default
        :return:
        """
        # get the name
        parent_name = self.get_parent_name(func_to_watch_for)
        metric_name = self.get_name(func_to_watch_for, metric_name)

        if metric_cls not in self.__metric_classes:
            raise ValueError(f'Wrong metric_cls {metric_cls}')

        # create a metric
        metric = self.__metric_classes[metric_cls](
            metric_name,
            metric_description or self.get_description(
                func_to_watch_for, parent_name
            ),
            labelnames=labelnames or [],
        )
        # raise error if a metric with the same name and hook (when) exists
        if metric_name in self.registry:
            stats_hook = self.registry[metric_name]
            if when in stats_hook.hooks_to_callbacks:
                raise ValueError(f'Duplicate hook {when}')
            stats_hook.hooks_to_callbacks[when] = callback
        else:
            stats_hook = StatsHook(
                metric=metric,
                wrapped_func_name=func_to_watch_for.__name__,
                hooks_to_callbacks={when: callback}
            )

        # save it and its callback
        self.registry[metric_name] = stats_hook

        # return a wrapper
        if when == 'before':
            wrapped_f = self.before_wrapper(func_to_watch_for, metric_name)
        elif when == 'after':
            wrapped_f = self.after_wrapper(func_to_watch_for, metric_name)
        else:
            raise ValueError(f'Wrong `when` type: {when}')

        return wrapped_f


metrics_registry = MetricsRegistry()
