# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from baskerville.features.updateable_features import UpdaterReplace


class FeatureCountry(UpdaterReplace):
    """
    The country of IP
    """
    COLUMNS = ['geoip_country_name']

    def __init__(self):
        super(FeatureCountry, self).__init__()

        self.group_by_aggs = {
            'country_first': F.first(F.col('geoip_country_name'))
        }
        self.columns_renamed = {
            'geoip.country_name': 'geoip_country_name'
        }

    @classmethod
    def spark_type(cls):
        return StringType()

    @classmethod
    def is_categorical(cls):
        return True

    def compute(self, df):
        df = df.withColumn(
            self.feature_name, F.col('country_first')
        )
        return df
