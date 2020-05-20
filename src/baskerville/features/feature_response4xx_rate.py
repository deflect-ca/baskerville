from baskerville.features.updateable_features import UpdaterRate
from baskerville.util.enums import FeatureComputeType
from pyspark.sql import functions as F

from baskerville.features.base_feature import TimeBasedFeature
from baskerville.features.feature_minutes_total import FeatureMinutesTotal
from baskerville.features.feature_response4xx_total import \
    FeatureResponse4xxTotal
from baskerville.features.helpers import update_rate


class FeatureResponse4xxRate(TimeBasedFeature, UpdaterRate):
    """
    For each IP compute the number of 4xx response codes per minute.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['http_response_code', '@timestamp']
    DEPENDENCIES = [FeatureMinutesTotal, FeatureResponse4xxTotal]

    def __init__(self):
        super(FeatureResponse4xxRate, self).__init__()
        self.group_by_aggs.update({
            '4xx': F.count(F.when(F.col('4xx') == True, F.col('4xx'))),  # noqa
        })
        self.pre_group_by_calcs.update({
            'response_code_category': F.floor(
                F.col('http_response_code') / 100.),
            '4xx': F.col('response_code_category') == 4,
        })

    def compute(self, df):
        df = df.withColumn(
            self.feature_name,
            F.when(F.col('dt') != 0.,
                   (F.col('4xx').cast('float') /
                    F.col('dt').cast('float')).cast('float')
                   ).otherwise(F.lit(self.feature_default).cast('float'))
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_rate(
                    past.get(FeatureResponse4xxTotal.feature_name_from_class()),
                    current[FeatureResponse4xxTotal.feature_name_from_class()],
                    current[FeatureMinutesTotal.feature_name_from_class()]
                )

    def update(self, df):
        return super().update(
            df,
            FeatureMinutesTotal.feature_name_from_class(),
            numerator=FeatureResponse4xxTotal.feature_name_from_class()
        )

