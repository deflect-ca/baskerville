from baskerville.features.updateable_features import UpdaterRate
from pyspark.sql import functions as F

from baskerville.features.base_feature import TimeBasedFeature
from baskerville.features.feature_minutes_total import FeatureMinutesTotal
from baskerville.features.feature_request_total import FeatureRequestTotal
from baskerville.features.helpers import update_rate


class FeatureRequestRate(TimeBasedFeature, UpdaterRate):
    """
    For each IP compute the number of requests per minute.
    """
    DEFAULT_VALUE = 1.
    COLUMNS = ['@timestamp']
    DEPENDENCIES = [FeatureMinutesTotal, FeatureRequestTotal]

    def __init__(self):
        super(FeatureRequestRate, self).__init__()

        self.group_by_aggs.update({
            'num_requests': F.count(F.col('@timestamp')).cast('float'),
        })

    def compute(self, df):

        df = df.withColumn(
            self.feature_name,
            F.when(
                F.col('dt') != 0.,
                (
                   F.col('num_requests').cast('float') /
                   F.col('dt').cast('float')
                ).cast('float')
           ).otherwise(F.lit(self.feature_default).cast('float'))
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_rate(
                    past.get(FeatureRequestTotal.feature_name_from_class()),
                    current[FeatureRequestTotal.feature_name_from_class()],
                    current[FeatureMinutesTotal.feature_name_from_class()]
                )

    def update(self, df):
        return super().update(
            df,
            numerator=FeatureRequestTotal.feature_name_from_class(),
            denominator=FeatureMinutesTotal.feature_name_from_class(),
        )
