from baskerville.features.updateable_features import UpdaterMean
from pyspark.sql import functions as F

from baskerville.features.feature_request_total import FeatureRequestTotal
from baskerville.features.helpers import update_mean


class FeatureGeoTimeAverage(UpdaterMean):
    """
    Use lat/lon and timestamp to find average local time of requesting IP.
    """
    DEFAULT_VALUE = 12.
    COLUMNS = ['@timestamp', 'geoip_location_lat', 'geoip_location_lon']
    DEPENDENCIES = [FeatureRequestTotal]

    def __init__(self):
        super(FeatureGeoTimeAverage, self).__init__()

        self.group_by_aggs = {
            'lat_first': F.first(F.col('geoip_location_lat')),
            'lon_first': F.first(F.col('geoip_location_lon')),
            'time_first': F.first(F.col('@timestamp'))
        }
        self.columns_renamed = {
            'geoip.location.lat': 'geoip_location_lat',
            'geoip.location.lon': 'geoip_location_lon',
        }

    def compute(self, df):
        from baskerville.spark.udfs import udf_compute_geotime
        from pyspark.sql import functions as F

        default = F.lit(self.feature_default)

        if 'lat_first' and 'lon_first':
            df = df.withColumn(self.feature_name, udf_compute_geotime(
                'lat_first', 'lon_first', 'time_first',
                default))
        else:
            df = df.withColumn(self.feature_name, default)

        df = df.withColumn(
            self.feature_name,
            F.col(self.feature_name).cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_mean(
            past.get(cls.feature_name_from_class()),
            current[cls.feature_name_from_class()],
            past.get(FeatureRequestTotal.feature_name_from_class()),
            current[FeatureRequestTotal.feature_name_from_class()]
        )

    def update(self, df, feat_column='features', old_feat_column='old_features'):
        return super().update(
            df,
            self.feature_name,
            FeatureRequestTotal.feature_name_from_class()
        )
