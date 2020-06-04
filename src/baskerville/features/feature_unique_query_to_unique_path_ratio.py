# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.features.updateable_features import UpdaterRatio
from pyspark.sql import functions as F

from baskerville.features.feature_unique_query_total import \
    FeatureUniqueQueryTotal
from baskerville.features.feature_unique_path_total import \
    FeatureUniquePathTotal
from baskerville.features.helpers import update_ratio


class FeatureUniqueQueryToUniquePathRatio(UpdaterRatio):
    """
    For each IP compute the ratio of unique queries to unique paths.
    """
    DEFAULT_VALUE = 1.
    COLUMNS = ['client_url', 'querystring']
    DEPENDENCIES = [FeatureUniqueQueryTotal, FeatureUniquePathTotal]

    def __init__(self):
        super(FeatureUniqueQueryToUniquePathRatio, self).__init__()

        self.group_by_aggs = {
            'num_unique_queries': (F.countDistinct(F.col('querystring'))),
            'unique_urls': F.countDistinct(F.col('client_url')),
        }

    def compute(self, df):
        df = df.withColumn(
            self.feature_name,
            (F.col('num_unique_queries').cast('float') /
             F.col('unique_urls').cast('float')).cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_ratio(
            past.get(FeatureUniqueQueryTotal.feature_name_from_class()),
            past.get(FeatureUniquePathTotal.feature_name_from_class()),
            current[FeatureUniqueQueryTotal.feature_name_from_class()],
            current[FeatureUniquePathTotal.feature_name_from_class()]
        )

    def update(self, df, feat_column='features', old_feat_column='old_features'):
        return super().update(
            df,
            FeatureUniqueQueryTotal.feature_name_from_class(),
            FeatureUniquePathTotal.feature_name_from_class(),
        )
