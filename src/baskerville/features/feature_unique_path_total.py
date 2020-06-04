# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.features.updateable_features import UpdaterTotal
from pyspark.sql import functions as F
from baskerville.features.helpers import update_total


class FeatureUniquePathTotal(UpdaterTotal):
    """
    For each IP compute the total number of unique paths requested.
    """
    DEFAULT_VALUE = 1.
    COLUMNS = ['client_url']
    DEPENDENCIES = []

    def __init__(self):
        super(FeatureUniquePathTotal, self).__init__()

        self.group_by_aggs = {
            'unique_urls': F.countDistinct(
                F.col('client_url')
            ),
        }

    def compute(self, df):
        from pyspark.sql import functions as F

        df = df.withColumn(
            self.feature_name,
            F.col('unique_urls').cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_total(
            current[cls.feature_name_from_class()],
            past.get(cls.feature_name_from_class(), 0.0)
        )
