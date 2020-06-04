# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.features.base_feature import UpdateableFeature
from baskerville.util.enums import FeatureComputeType
from pyspark.sql import functions as F


class UpdaterTotal(UpdateableFeature):
    _update_type = FeatureComputeType.total

    def update(self, df):
        return df.withColumn(
            self.updated_feature_col_name,
            (
                F.col(self.past_features_column)[self.feature_name] +
                F.col(self.current_features_column)[self.feature_name]
            ).cast('float')
        )


class UpdaterRate(UpdateableFeature):
    _update_type = FeatureComputeType.rate

    def update(self, df, denominator, numerator=None):
        if not numerator:
            numerator = self.feature_name
        past = F.col(self.past_features_column)[numerator]
        current = F.col(self.current_features_column)[numerator]
        denominator = F.col(self.current_features_column)[denominator]

        return df.withColumn(
            self.updated_feature_col_name,
            (F.when(
                denominator > 0, ((past + current) / denominator)
            ).otherwise(
                current
            )).cast('float')
        )


class UpdaterMaximum(UpdateableFeature):
    _update_type = FeatureComputeType.maximum

    def update(self, df, past, current):
        return df.withColumn(
            self.updated_feature_col_name,
            F.greatest(past, current)
        )


class UpdaterMinimum(UpdateableFeature):
    _update_type = FeatureComputeType.minimum

    def update(self, df, past, current):
        return df.withColumn(
            self.updated_feature_col_name,
            F.least(past, current)
        )


class UpdaterReplace(UpdateableFeature):
    _update_type = FeatureComputeType.replace

    def update(self, df, current=None):
        if not current:
            current = self.feature_name

        current_col = F.col(self.current_features_column)[current]

        return df.withColumn(self.updated_feature_col_name, current_col)


class UpdaterRatio(UpdateableFeature):
    _update_type = FeatureComputeType.ratio

    def update(self, df, numerator, denominator):
        past_value = F.col(self.past_features_column)[numerator]
        current_value = F.col(self.current_features_column)[numerator]
        past_denom = F.col(self.past_features_column)[denominator]
        current_denom = F.col(self.current_features_column)[denominator]

        return df.withColumn(
            self.updated_feature_col_name,
            (F.when(
                (past_denom + current_denom) > 0,
                (past_value + current_value) / (past_denom + current_denom)
            ).otherwise(
                (past_value + current_value) / F.lit(0.01)
            )
            ).cast('float')
        )


class UpdaterMean(UpdateableFeature):
    _update_type = FeatureComputeType.mean

    def update(self, df, numerator, denominator):
        past_value = F.col(self.past_features_column)[numerator]
        current_value = F.col(self.current_features_column)[numerator]
        past_denom = F.col(self.past_features_column)[denominator]
        current_denom = F.col(self.current_features_column)[denominator]
        return df.withColumn(
            self.updated_feature_col_name,
            (
                (past_denom * past_value + current_denom * current_value) /
                (past_denom + current_denom)
            ).cast('float')
        )


class UpdaterVariance(UpdateableFeature):
    _update_type = FeatureComputeType.variance

    def update(self, df, value_col, count_col, mean_col):
        past_value = F.col(self.past_features_column)[value_col]
        current_value = F.col(self.current_features_column)[value_col]
        past_count = F.col(self.past_features_column)[count_col]
        current_count = F.col(self.current_features_column)[count_col]
        past_mean = F.col(self.past_features_column)[mean_col]
        current_mean = F.col(self.current_features_column)[mean_col]

        updated_mean = (
            past_count * past_value + current_count * current_value
        ) / (past_count + current_count)

        updated_past_counts = (past_count - 1) * F.pow(past_value, F.lit(2))
        updated_current_counts = (current_count - 1) * F.pow(
            current_value, F.lit(2)
        )
        updated_past_means = past_count * F.pow(
            (past_mean - updated_mean), F.lit(2)
        )
        updated_current_means = current_count * F.pow(
            (current_mean - updated_mean), F.lit(2)
        )
        population_size = past_count + current_count - 1

        return df.withColumn(
            self.updated_feature_col_name,
            ((
                updated_past_counts +
                updated_current_counts +
                updated_past_means +
                updated_current_means
            ) / population_size).cast('float')
        )
