from baskerville.features.updateable_features import UpdaterRatio
from pyspark.sql import functions as F

from baskerville.features.feature_top_page_total import FeatureTopPageTotal
from baskerville.features.feature_request_total import FeatureRequestTotal
from baskerville.features.helpers import update_ratio


class FeatureTopPageToRequestRatio(UpdaterRatio):
    """
    For each IP compute the ratio of the number of requests to the
    most-queried page to the total number of requests.
    """
    DEFAULT_VALUE = 1.
    COLUMNS = ['client_url', '@timestamp']
    DEPENDENCIES = [FeatureTopPageTotal, FeatureRequestTotal]

    def __init__(self):
        super(FeatureTopPageToRequestRatio, self).__init__()

        self.group_by_aggs = {
            'num_requests': F.count(F.col('@timestamp')),
            'max_client_url_value_count': F.max(
                F.col('client_url_value_count')
            )
        }

    def misc_compute(self, logs_df):
        if 'client_url_value_count' in logs_df.columns:
            return logs_df

        df = logs_df.groupby(
            'client_request_host', 'client_ip', 'client_url'
        ).count(
        ).withColumnRenamed('count', 'client_url_value_count')

        logs_df = logs_df.join(
            df,
            on=[
                'client_request_host',
                'client_ip',
                'client_url'
            ],
            how='left'
        )
        return logs_df

    def compute(self, df):
        df = df.withColumn(
            self.feature_name,
            (F.col('max_client_url_value_count').cast('float') /
             F.col('num_requests').cast('float')).cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_ratio(
                    past.get(FeatureTopPageTotal.feature_name_from_class()),
                    past.get(FeatureRequestTotal.feature_name_from_class()),
                    current[FeatureTopPageTotal.feature_name_from_class()],
                    current[FeatureRequestTotal.feature_name_from_class()]
                )

    def update(self, df, feat_column='features', old_feat_column='old_features'):
        return super().update(
            df,
            FeatureTopPageTotal.feature_name_from_class(),
            FeatureRequestTotal.feature_name_from_class(),
        )

