# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from baskerville.features.updateable_features import UpdaterReplace


class FeatureHostCountry(UpdaterReplace):
    """
    The host + country of IP
    """
    COLUMNS = ['geoip_country_name_hc', 'client_request_host']

    def __init__(self):
        super(FeatureHostCountry, self).__init__()

        self.group_by_aggs = {
            'country_first_hc': F.first(F.col('geoip_country_name_hc')),
            'host_first_hc': F.first(F.col('client_request_host'))
        }
        self.columns_renamed = {
            'geoip.country_name': 'geoip_country_name_hc'
        }

    @classmethod
    def spark_type(cls):
        return StringType()

    @classmethod
    def is_categorical(cls):
        return True

    def compute(self, df):
        df = df.withColumn(
            self.feature_name,
            F.concat(F.col("host_first_hc"), F.lit("_"), F.col("country_first_hc"))
        )
        return df
