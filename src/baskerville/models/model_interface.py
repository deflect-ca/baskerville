# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import inspect
import logging


class ModelInterface(object):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_param_names(self):
        return list(inspect.signature(self.__init__).parameters.keys())

    def set_params(self, **params):
        param_names = self.get_param_names()
        for key, value in params.items():
            if key not in param_names:
                raise RuntimeError(
                    f'Class {self.__class__.__name__} does not '
                    f'have {key} attribute')
            setattr(self, key, value)

    def get_params(self):
        params = {}
        for name in self.get_param_names():
            params[name] = getattr(self, name)
        return params

    def _get_class_path(self):
        return f'{self.__class__.__module__}.{self.__class__.__name__}'

    def train(self, df):
        pass

    def predict(self, df):
        pass

    def save(self, path, spark_session=None):
        pass

    def load(self, path, spark_session=None):
        pass

    def set_logger(self, logger):
        self.logger = logger
