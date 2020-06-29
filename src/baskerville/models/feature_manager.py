# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import itertools
from collections import OrderedDict

from baskerville.features.helpers import feature_is_updateable
from baskerville.util.helpers import get_logger


class FeatureManager(object):
    def __init__(
            self,
            engine_conf
    ):
        self.all_features = engine_conf.all_features
        self.extra_features = engine_conf.extra_features
        self.active_features = None
        self.active_feature_names = None
        self.updateable_active_features = None
        self.active_columns = None
        self.update_feature_cols = None
        self.column_renamings = None
        self.pre_group_by_calculations = None
        self.post_group_by_calculations = None

        self.logger = get_logger(
            self.__class__.__name__,
            logging_level=engine_conf.log_level,
            output_file=engine_conf.logpath
        )

    def initialize(self):
        self.active_features = self.get_active_features()
        self.active_feature_names = self.get_active_feature_names()
        self.updateable_active_features = self.get_updateable_active_features()
        self.active_columns = self.get_active_columns()
        self.update_feature_cols = self.get_update_feature_columns()
        self.column_renamings = self.get_feature_col_renamings()
        self.pre_group_by_calculations = self.get_feature_pre_group_by_calcs()
        self.post_group_by_calculations = self.get_post_group_by_calculations()

    def get_active_features(self):
        """
        Returns a list of the active feature instances
        :return:
        """
        feature_list = self.extra_features
        if not feature_list:
            raise RuntimeError('No features specified! Either input model '
                               'or specify features in config.')

        feature_list = set(feature_list)
        diff = feature_list.difference(self.all_features.keys())
        if diff:
            self.logger.warn(
                f'Warning: omitting unknown features {diff}.'
            )
        features_to_use = [f for n, f in self.all_features.items()
                           if n in feature_list]
        dependencies = list(set(
            f for f in itertools.chain(
                *[f.DEPENDENCIES for f in features_to_use]  # todo class attr
            )
        ))
        features_to_use += dependencies
        features_to_use = set(features_to_use)
        return list(set([f() for f in features_to_use]))

    def get_active_feature_names(self) -> list:
        """
        Returns a list of the active feature names
        :return:
        """
        return [f.feature_name for f in self.active_features]

    def get_updateable_active_features(self) -> list:
        """
        Returns a list of the updateable features
        :return:
        """
        return [
            f for f in self.active_features
            if feature_is_updateable(f)
        ]

    def get_active_columns(self) -> list:
        """
        returns a list of the column names that need to be in the logs df
        :return:
        :rtype:list[str]
        """
        return list(
            set(
                itertools.chain(
                    *[feature.columns for feature in self.active_features]
                )
            )
        )

    def get_update_feature_columns(self) -> list:
        """
        Returns the names for the updated features
        :return:
        """
        return [
            f.updated_feature_col_name for f in self.active_features
            if feature_is_updateable(f)
        ]

    def feature_config_is_valid(self) -> bool:
        """
        Returns False if any of the feature checks fail and True otherwise.
        If there is no model, then we don't do feature checks. The reason
        behind this is because we want to distinguish whether we cannot predict
        because we're missing the ml model or because the feature configuration
        is not valid.
        :return:
        """
        checks = []
        checks.append(self.feature_dependencies_met())

        return False not in checks

    def feature_dependencies_met(self) -> bool:
        """
        Checks that the features defined as dependencies are included in the
        active features list
        :return: True if all feature dependencies are met, False otherwise
        :rtype: bool
        """
        required_features = set(itertools.chain(
            *[feature.dependencies for feature in self.active_features])
        )
        return set(
            [f.feature_name_from_class() for f in required_features]
        ).issubset(set(self.active_feature_names))

    def get_feature_pre_group_by_calcs(self) -> dict:
        """
        Gather all the calculations that need to be applied on the logs_df
        :return:
        :rtype: dict[str, pyspark.sql.Column]
        """
        aggs = OrderedDict()

        for f in self.active_features:
            for k, v in f.pre_group_by_calcs.items():
                if k not in aggs:
                    aggs[k] = (v).alias(k)

        return aggs

    def get_feature_group_by_aggs(self) -> dict:
        """
        Gather all the aggregations that need to be applied to the grouped
        dataframe
        :return:
        :rtype: dict[str, pyspark.sql.Column]
        """
        aggs = OrderedDict()

        for f in self.active_features:
            for k, v in f.group_by_aggs.items():
                if k not in aggs:
                    aggs[k] = (v).alias(k)

        return aggs

    def get_feature_col_renamings(self) -> list:
        """
        Gather all the column renamings that need to be applied on the logs_df
        :return:
        :rtype: list[tuple(str, pyspark.sql.Column)]
        """
        renamings = []

        for f in self.active_features:
            for k, v in f.columns_renamed.items():
                if (k, v) not in renamings:
                    renamings.append((k, v))
                else:
                    self.logger.warn(f'Duplicate column renaming {k} to {v}')

        return renamings

    def get_post_group_by_calculations(self) -> dict:
        """
        Gather all the post group by calculations that are defined by the
        features
        :return:
        :rtype: dict[str, pyspark.sql.Column]
        """
        post_group_by_columns = {}

        for feature in self.active_features:
            for k, v in feature.post_group_by_calcs.items():
                if k not in post_group_by_columns:
                    post_group_by_columns[k] = v
                else:
                    self.logger.debug(f'{k} is already in the dataframe.')

        return post_group_by_columns
