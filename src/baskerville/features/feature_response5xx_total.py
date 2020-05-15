from baskerville.features.updateable_features import UpdaterTotal
from baskerville.util.enums import FeatureComputeType
from pyspark.sql import functions as F

from baskerville.features.base_feature import BaseFeature
from baskerville.features.helpers import update_total


class FeatureResponse5xxTotal(UpdaterTotal):
    """
    For each IP compute the total number of 5xx response codes.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['http_response_code']
    DEPENDENCIES = []

    def __init__(self):
        super(FeatureResponse5xxTotal, self).__init__()

        self.group_by_aggs = {
            '5xx': F.count(F.when(F.col('5xx') == True, F.col('5xx')))  # noqa
        }
        self.pre_group_by_calcs = {
            'response_code_category': F.floor(
                F.col('http_response_code') / 100.),
            '5xx': F.col('response_code_category') == 5,
        }

    def compute(self, df):
        from pyspark.sql import functions as F

        df = df.withColumn(
            self.feature_name,
            (F.col('5xx')).cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_total(
            current[cls.feature_name_from_class()],
            past.get(cls.feature_name_from_class())
        )
