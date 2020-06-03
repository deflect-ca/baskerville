from baskerville.features.updateable_features import UpdaterTotal
from pyspark.sql import functions as F
from baskerville.features.helpers import update_total


class FeatureTopPageTotal(UpdaterTotal):
    """
    For each IP compute the number of requests to the most-queried page.
    """
    DEFAULT_VALUE = 1.
    COLUMNS = ['client_url']
    DEPENDENCIES = []

    def __init__(self):
        super(FeatureTopPageTotal, self).__init__()

        self.group_by_aggs = {
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
            F.col('max_client_url_value_count').cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_total(
            current[cls.feature_name_from_class()],
            past.get(cls.feature_name_from_class(), 0.0)
        )
