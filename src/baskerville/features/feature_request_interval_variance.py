from baskerville.features.updateable_features import UpdaterVariance
from pyspark.sql import functions as F
from pyspark.sql import Window

from baskerville.features.feature_request_total import FeatureRequestTotal
from baskerville.features.feature_request_interval_average import \
    FeatureRequestIntervalAverage
from baskerville.features.helpers import update_variance


class FeatureRequestIntervalVariance(UpdaterVariance):
    """
    For each IP compute the variance of the time interval between subsequent
    requests (in minutes).
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['@timestamp']
    DEPENDENCIES = [FeatureRequestTotal, FeatureRequestIntervalAverage]

    def __init__(self):
        super(FeatureRequestIntervalVariance, self).__init__()

        self.w = Window.partitionBy(
                    F.col('client_request_host'), F.col('client_ip')
                ).orderBy(F.col("@timestamp"))
        self.group_by_aggs = {
            'request_interval_var':  F.variance(
                F.col('request_interval').cast('float') / 60.
            ),
        }
        self.pre_group_by_calcs = {
            'row_num_per_group':
            F.row_number().over(self.w),
            'prev_ts': F.lag(F.col('@timestamp')).over(
                self.w
            ),
            'request_interval': F.when(
                F.col('row_num_per_group') > 1,
                F.when(
                    F.isnull(
                        F.col('@timestamp').cast('long') -
                        F.col('prev_ts').cast('long')
                    ), 0
                ).otherwise(
                    F.col('@timestamp').cast('long') -
                    F.col('prev_ts').cast('long')
                )
            ).otherwise(None),
        }

    def compute(self, df):
        from pyspark.sql import functions as F

        df = df.withColumn(
            self.feature_name,
            F.when(
                F.isnan(F.col('request_interval_var')) |
                F.isnull(F.col('request_interval_var')),
                F.lit(self.feature_default).cast('float')
            ).otherwise(F.col('request_interval_var').cast('float'))
        ).fillna({self.feature_name: self.feature_default})
        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_variance(
            past.get(cls.feature_name_from_class()),
            current[cls.feature_name_from_class()],
            past.get(FeatureRequestTotal.feature_name_from_class()),
            current[FeatureRequestTotal.feature_name_from_class()],
            past.get(FeatureRequestIntervalAverage.feature_name_from_class()),
            current[FeatureRequestIntervalAverage.feature_name_from_class()]
        )

    def update(self, df, feat_column='features', old_feat_column='old_features'):
        return super().update(
            df,
            self.feature_name,
            FeatureRequestTotal.feature_name_from_class(),
            FeatureRequestIntervalAverage.feature_name_from_class()
        )

