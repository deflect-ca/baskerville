import abc
from functools import wraps

from attr import dataclass
from baskerville.util.helpers import get_logger
from prometheus_client import Summary, Counter, Gauge
from prometheus_client.core import Metric


class Stats(object):
    _prefix = 'baskerville_'
    __registry = {}

    @property
    def registry(self):
        return self.__registry

    def prefix(self, name):
        return f'{self._prefix}{name}'

    @staticmethod
    def get_parent_name(func):
        """
        Returns the class's name if any, else emtpy.
        :param callable func: the callable to get the class's name for.
        :return: the class's name if a bound method else an empty string
        :rtype: str
        """
        return func.__self__.__class__.__name__ \
            if hasattr(func, '__self__') else ''

    def get_name(self, func, name=None):
        """
        Returns the metric name
        :param callable func:
        :return:
        :rtype:str
        """

        if name:
            return self.prefix(name)
        return f'{self._prefix}{func.__name__}'

    @staticmethod
    def get_description(func, parent_name):
        return f'Stats for {parent_name} "{func.__name__}"'

    @abc.abstractmethod
    def register(self, *args, **kwargs):
        pass


class PerformanceStatsRegistry(Stats):
    _prefix = 'baskerville_performance_'

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    @staticmethod
    def get_description(f, parent_name):
        return f'Time Performance of {parent_name} "{f.__name__}"'

    def register(self, f, name=None):
        parent_name = self.get_parent_name(f)

        name = self.get_name(f, name)
        # todo: different objects with same method names will not be registered
        if name in self.registry:
            raise ValueError(f'Duplicate name: {name}')

        summary = Summary(
            name,
            self.get_description(f, parent_name)
        )

        self.registry[name] = summary

        @wraps(f)
        def wrapper_f(*args, **kwargs):
            with summary.time():
                result = f(*args, **kwargs)
            return result

        return wrapper_f


class ProgressStatsRegistry(Stats):
    _prefix = 'baskerville_progress_'

    def __init__(self):
        self.__registry = {}
        self.logger = get_logger(self.__class__.__name__)

    @staticmethod
    def get_description(f, parent_name):
        return f'Time Performance of {parent_name} "{f.__name__}"'

    def before_wrapper(self, func, name):
        @wraps(func)
        def wrapper_f(*args, **kwargs):
            stats_h = self.__registry[name]
            stats_h.hooks_to_callbacks['before'](stats_h.metric, func.__self__)
            return func(*args, **kwargs)

        return wrapper_f

    def after_wrapper(self, func, name):

        @wraps(func)
        def wrapper_f(*args, **kwargs):
            result = func(*args, **kwargs)
            stats_h = self.__registry[name]
            stats_h.hooks_to_callbacks['after'](stats_h.metric, func.__self__)
            return result

        return wrapper_f

    def register_action_hook(
            self,
            func_to_watch_for,
            callback,
            when='after',
            metric_name=None,
            metric_cls=Counter,
            labelnames=None,
    ):
        """
        Add metrics by wrapping bound methods and executing the user defined
        callback function depending on the when value. For now this only
        supports bound methods.
        :param callable func_to_watch_for: the bound methond to wrap
        :param callable callback: a function that will be executed before or
        after the execution of func_to_watch_for
        :param str when: 'before' or 'after', indicates when to run the metric
        collection callback
        :param str metric_name: the name of the metric
        :param prometheus_client[Summary, Counter, Gauge, Histogram] metric_cls
        : the prometheus_client metric class
        :param list labelnames: the label names if any
        :return: None
        """
        # get the name
        parent_name = self.get_parent_name(func_to_watch_for)
        metric_name = self.get_name(func_to_watch_for, metric_name)

        # create a metric
        metric = metric_cls(
            metric_name,
            self.get_description(func_to_watch_for, parent_name),
            labelnames=labelnames,
        )
        # raise error if a metric with the same name and hook (when) exists
        if metric_name in self.__registry:
            stats_hook = self.__registry[metric_name]
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
        self.__registry[metric_name] = stats_hook

        # return a wrapper
        if when == 'before':
            wrapped_f = self.before_wrapper(func_to_watch_for, metric_name)
        elif when == 'after':
            wrapped_f = self.after_wrapper(func_to_watch_for, metric_name)
        else:
            raise ValueError(f'Wrong when type: {when}')

        # restore __self__ because wrapping loses it.
        wrapped_f.__self__ = func_to_watch_for.__self__
        return wrapped_f

    def register(self, name, func, metric_cls=Gauge):
        """
        # TODO: inconsistent with the above...
        :param name:
        :param func:
        :param metric_cls:
        :return:
        """
        name = self.prefix(name)
        if name in self.__registry:
            raise ValueError(f'Duplicate name: {name}')
        metric = metric_cls(name, name)
        if hasattr(metric, 'set_function'):
            metric.set_function(func)
        else:
            raise NotImplementedError(
                f'Not implemented for metric_cls {metric_cls}'
            )
        self.__registry[name] = StatsHook(
            metric=metric,
            wrapped_func_name=None,
            hooks_to_callbacks={'any': func}
        )
