# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import abc
import datetime
from collections import OrderedDict
from typing import List

import pyspark

from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.service_provider import ServiceProvider
from baskerville.util.helpers import get_logger, TimeBucket


class Task(object, metaclass=abc.ABCMeta):
    name: str
    df: pyspark.sql.DataFrame
    steps: List['Task']
    config: BaskervilleConfig

    def __init__(self, config: BaskervilleConfig, steps: list = ()):
        self.df = None
        self.config = config
        self.steps = steps
        self.step_to_action = OrderedDict({
            str(step): step for step in self.steps
        })
        self.logger = get_logger(
            self.__class__.__name__,
            logging_level=self.config.engine.log_level,
            output_file=self.config.engine.logpath
        )
        self.time_bucket = TimeBucket(self.config.engine.time_bucket)
        self.start_time = datetime.datetime.utcnow()
        self.service_provider = ServiceProvider(self.config)

    def __str__(self):
        name = f'{self.__class__.__name__} '
        if self.steps:
            name += ",".join(step.__class__.__name__ for step in self.steps)
        return name

    def __getattr__(self, item):
        """
        Look into service provider if attribute not found in current class
        """
        if hasattr(self.service_provider, item):
            return getattr(self.service_provider, item)
        raise AttributeError(f'No such attribute {item}')

    @property
    def spark(self):
        return self.service_provider.spark

    @property
    def runtime(self):
        return self.service_provider.runtime

    @runtime.setter
    def runtime(self, value):
        self.service_provider.runtime = value

    @property
    def tools(self):
        return self.service_provider.tools

    @property
    def db_tools(self):
        return self.service_provider.tools

    def set_df(self, df):
        self.df = df
        return self

    def initialize(self):
        self.service_provider.initialize_db_tools_service()
        self.service_provider.initialize_spark_service()

        for step in self.steps:
            step.initialize()

    def run(self):
        """
        For all defined steps, run the respective actions
        :return: returns the dataframe (self.df) after all the computations.
        :rtype: pyspark.sql.DataFrame
        """
        self.remaining_steps = list(self.step_to_action.keys())
        for descr, task in self.step_to_action.items():
            self.logger.info('Starting step {}'.format(descr))
            self.df = task.set_df(self.df).run()
            self.logger.info('Completed step {}'.format(descr))
            self.remaining_steps.remove(descr)
        return self.df

    def finish_up(self):
        self.service_provider.finish_up()

    def reset(self):
        self.service_provider.reset()


class CacheTask(Task):
    """
    A task that utilizes RequestSetCache
    """
    def __init__(self, config: BaskervilleConfig, steps: list = ()):
        super().__init__(config, steps)

    def initialize(self):
        super().initialize()
        self.service_provider.initialize_request_set_cache_service()

    @property
    def request_set_cache(self):
        return self.service_provider.request_set_cache


class MLTask(CacheTask):
    """
    A task that uses AnomalyModel, Model and FeatureManager
    """
    def __init__(self, config: BaskervilleConfig, steps: list = ()):
        super().__init__(config, steps)

    def initialize(self):
        super().initialize()
        self.service_provider.initalize_ml_services()

    @property
    def model(self):
        return self.service_provider.model

    @property
    def model_index(self):
        return self.service_provider.model_index

    @property
    def feature_manager(self):
        return self.service_provider.feature_manager
