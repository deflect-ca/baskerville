# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.features.updateable_features import UpdaterTotal
from pyspark.sql import functions as F
from baskerville.features.helpers import update_total


class FeatureUniqueUaTotal(UpdaterTotal):
    """
    For each IP compute the total number of different user agents used.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['client_ua']
    DEPENDENCIES = []

    def __init__(self):
        """
        Call the parent constructor.
        """
        super(FeatureUniqueUaTotal, self).__init__()

        self.group_by_aggs = {
            'distinct_ua': F.countDistinct(F.col('client_ua')).cast('float')
        }

    def compute(self, df):
        df = df.withColumn(
            self.feature_name,
            F.col('distinct_ua').cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_total(
            current[cls.feature_name_from_class()],
            past.get(cls.feature_name_from_class())
        )
