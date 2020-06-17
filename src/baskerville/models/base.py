# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import abc

from baskerville.db import get_jdbc_url
from baskerville.models.feature_manager import FeatureManager
from baskerville.util.helpers import get_logger


class BaskervilleBase(object, metaclass=abc.ABCMeta):

    def __init__(self, config):
        self.config = config

    @abc.abstractmethod
    def run(self, *args, **kwargs):
        pass


class PipelineBase(object, metaclass=abc.ABCMeta):
    """
    The base contract for all pipelines:
    Basic attributes:
    - runtime
    - all_features
    - active_features
    - engine_conf
    - db_conf
    - db_url
    - step_to_action
    - remaining_steps
    - logs_df

    Contract methods:
    - run
    - finish_up: all pipelines must clean up after themselves
    """

    def __init__(self, db_conf, engine_conf, clean_up):
        self.runtime = None
        # todo: does not belong here anymore - see feature manager
        self.active_features = None
        self.step_to_action = None
        self.remaining_steps = None
        self.logs_df = None  # todo: remove once refactoring is done
        self.df = None
        self.db_conf = db_conf
        self.engine_conf = engine_conf
        self.all_features = self.engine_conf.all_features
        self.clean_up = clean_up
        self.db_url = get_jdbc_url(self.db_conf)
        self.logger = get_logger(
            self.__class__.__name__,
            logging_level=self.engine_conf.log_level,
            output_file=self.engine_conf.logpath
        )

    @abc.abstractmethod
    def run(self, *args, **kwargs):
        """
        For all defined steps, run the respective actions
        :param args:
        :param kwargs:
        :return:
        """
        for step, action in self.step_to_action.items():
            self.logger.info('Starting step {}'.format(step))
            action()
            self.logger.info('Completed step {}'.format(step))
            self.remaining_steps.remove(step)

    @abc.abstractmethod
    def initialize(self):
        pass

    @abc.abstractmethod
    def finish_up(self):
        pass


class TrainingPipelineBase(PipelineBase, metaclass=abc.ABCMeta):
    def __init__(self, db_conf, engine_conf, spark_conf, clean_up=True):
        super().__init__(db_conf, engine_conf, clean_up)
        self.data = None
        self.training_conf = self.engine_conf.training
        self.spark_conf = spark_conf
        self.feature_manager = FeatureManager(self.engine_conf)

    @abc.abstractmethod
    def get_data(self):
        pass

    @abc.abstractmethod
    def train(self):
        pass

    @abc.abstractmethod
    def test(self):
        pass

    @abc.abstractmethod
    def evaluate(self):
        pass

    @abc.abstractmethod
    def save(self, *args, **kwargs):
        pass

    def run(self, *args, **kwargs):
        super().run()
