# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.features.updateable_features import UpdaterTotal
from pyspark.sql import functions as F

from baskerville.features.helpers import update_total


class FeatureJsTotal(UpdaterTotal):
    """
    For each IP compute the total js requests.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['content_type']
    DEPENDENCIES = []

    def __init__(self):
        super(FeatureJsTotal, self).__init__()

        self.group_by_aggs = {
            'js_count': F.count(F.when(F.col('is_js') == True,  # noqa
                                       F.col('is_js')))
        }
        self.pre_group_by_calcs = {
            'is_js': F.array_contains(F.split(F.col(
                'content_type'), '/'
            ), 'javascript')
        }

    def compute(self, df):
        from pyspark.sql import functions as F

        df = df.withColumn(
            self.feature_name,
            F.col('js_count').cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_total(
            past.get(cls.feature_name_from_class()),
            current[cls.feature_name_from_class()]
        )
