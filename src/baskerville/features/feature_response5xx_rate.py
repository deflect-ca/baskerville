from baskerville.features.updateable_features import UpdaterRate
from pyspark.sql import functions as F

from baskerville.features.base_feature import TimeBasedFeature
from baskerville.features.feature_minutes_total import FeatureMinutesTotal
from baskerville.features.feature_response5xx_total import \
    FeatureResponse5xxTotal
from baskerville.features.helpers import update_rate


class FeatureResponse5xxRate(TimeBasedFeature, UpdaterRate):
    """
    For each IP compute the number of 5xx response codes per minute.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['http_response_code', '@timestamp']
    DEPENDENCIES = [FeatureMinutesTotal, FeatureResponse5xxTotal]

    def __init__(self):
        super(FeatureResponse5xxRate, self).__init__()
        self.group_by_aggs.update({
            '5xx': F.count(F.when(F.col('5xx') == True, F.col('5xx'))),  # noqa
        })
        self.pre_group_by_calcs = {
            'response_code_category': F.floor(
                F.col('http_response_code') / 100.),
            '5xx': F.col('response_code_category') == 5,
        }

    def compute(self, df):
        df = df.withColumn(
            self.feature_name,
            F.when(F.col('dt') != 0.,
                   (F.col('5xx').cast('float') /
                    F.col('dt').cast('float')).cast('float')
                   ).otherwise(F.lit(self.feature_default).cast('float'))
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_rate(
            past.get(FeatureResponse5xxTotal.feature_name_from_class()),
            current[FeatureResponse5xxTotal.feature_name_from_class()],
            current[FeatureMinutesTotal.feature_name_from_class()]
        )

    def update(self, df):
        return super().update(
            df,
            FeatureMinutesTotal.feature_name_from_class(),
            numerator=FeatureResponse5xxTotal.feature_name_from_class()
        )
