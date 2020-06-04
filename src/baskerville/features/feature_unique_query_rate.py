from baskerville.features.updateable_features import UpdaterRate
from pyspark.sql import functions as F

from baskerville.features.base_feature import TimeBasedFeature
from baskerville.features.feature_minutes_total import FeatureMinutesTotal
from baskerville.features.feature_unique_query_total import \
    FeatureUniqueQueryTotal
from baskerville.features.helpers import update_rate


class FeatureUniqueQueryRate(TimeBasedFeature, UpdaterRate):
    """
    For each IP compute the total number of unique queries per minute.
    """
    DEFAULT_VALUE = 1.
    COLUMNS = ['querystring', '@timestamp']
    DEPENDENCIES = [FeatureMinutesTotal, FeatureUniqueQueryTotal]

    def __init__(self):
        super(FeatureUniqueQueryRate, self).__init__()

        self.group_by_aggs.update({
            'num_unique_queries': (F.countDistinct(F.col('querystring'))),
        })

    def compute(self, df):
        df = df.withColumn(
            self.feature_name,
            F.when(F.col('dt') != 0.,
                   (F.col('num_unique_queries').cast('float') /
                    F.col('dt').cast('float')).cast('float')
                   ).otherwise(F.lit(self.feature_default).cast('float'))
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_rate(
            past.get(FeatureUniqueQueryTotal.feature_name_from_class()),
            current[FeatureUniqueQueryTotal.feature_name_from_class()],
            current[FeatureMinutesTotal.feature_name_from_class()]
        )

    def update(self, df):
        return super().update(
            df,
            FeatureMinutesTotal.feature_name_from_class(),
            numerator=FeatureUniqueQueryTotal.feature_name_from_class()
        )
