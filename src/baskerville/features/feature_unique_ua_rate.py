# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.features.updateable_features import UpdaterRate
from pyspark.sql import functions as F

from baskerville.features.base_feature import TimeBasedFeature
from baskerville.features.feature_minutes_total import FeatureMinutesTotal
from baskerville.features.feature_unique_ua_total import FeatureUniqueUaTotal
from baskerville.features.helpers import update_rate


class FeatureUniqueUaRate(TimeBasedFeature, UpdaterRate):
    """
    For each IP compute the number of unique user agents used per minute.
    """
    DEFAULT_VALUE = 1.
    COLUMNS = ['client_ua', '@timestamp']
    DEPENDENCIES = [FeatureMinutesTotal, FeatureUniqueUaTotal]

    def __init__(self):
        """
        Call the parent constructor.
        """
        super(FeatureUniqueUaRate, self).__init__()

        self.group_by_aggs.update({
            'distinct_ua': F.countDistinct(F.col('client_ua')).cast('float'),
        })

    def compute(self, df):
        df = df.withColumn(
            self.feature_name,
            F.when(F.col('dt') != 0.,
                   (F.col('distinct_ua').cast('float') /
                    F.col('dt').cast('float')).cast('float')
                   ).otherwise(F.lit(self.feature_default).cast('float'))
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_rate(
            past.get(FeatureUniqueUaTotal.feature_name_from_class()),
            current[FeatureUniqueUaTotal.feature_name_from_class()],
            current[FeatureMinutesTotal.feature_name_from_class()]
        )

    def update(self, df):
        return super().update(
            df,
            FeatureMinutesTotal.feature_name_from_class(),
            numerator=FeatureUniqueUaTotal.feature_name_from_class()
        )
