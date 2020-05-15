from baskerville.features.feature_geo_time_average import FeatureGeoTimeAverage
from baskerville.features.feature_request_total import FeatureRequestTotal
from baskerville.util.enums import FeatureComputeType
from pyspark.sql import functions as F, types as T

from tests.unit.baskerville_tests.helpers.spark_testing_base import \
    FeatureSparkTestCase


class TestSparkGeoTimeAverage(FeatureSparkTestCase):

    def setUp(self):
        super(TestSparkGeoTimeAverage, self).setUp()
        self.feature = FeatureGeoTimeAverage()

    def test_instance(self):
        self.assertTrue(hasattr(self.feature, 'feature_name'))
        self.assertTrue(hasattr(self.feature, 'COLUMNS'))
        self.assertTrue(hasattr(self.feature, 'DEPENDENCIES'))
        self.assertTrue(hasattr(self.feature, 'DEFAULT_VALUE'))
        self.assertTrue(hasattr(self.feature, 'compute_type'))

        self.assertTrue(self.feature.feature_name == 'geo_time_average')
        self.assertTrue(
            self.feature.columns == ['@timestamp', 'geoip_location_lat',
                                     'geoip_location_lon'])
        self.assertTrue(self.feature.dependencies == [FeatureRequestTotal])
        self.assertTrue(self.feature.DEFAULT_VALUE == 12.)
        self.assertTrue(self.feature.compute_type == FeatureComputeType.mean)
        self.assertIsNotNone(self.feature.feature_name)
        self.assertIsNotNone(self.feature.feature_default)

        self.assertTrue(isinstance(self.feature.feature_name, str))
        self.assertTrue(isinstance(self.feature.feature_default, float))

    def test_compute_single_record(self):
        ats_record = {
            'client_ip': '55.555.55.55',
            '@timestamp': '2018-01-17T08:30:00.000Z',
            'geoip.location.lat': 51.5074,
            'geoip.location.lon': 0.1278,
        }
        sub_df = self.get_sub_df_for_feature(
            self.feature,
            [
                ats_record,
            ]
        )
        result = self.feature.compute(sub_df)

        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(8.5).cast('float')
        )

        for k, v in self.feature.columns_renamed.items():
            expected_df = expected_df.withColumnRenamed(k, v)
        expected_df = self.schema_helper(
            expected_df, result.schema, [self.feature.feature_name]
        )
        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_compute_multiple_records(self):
        from pyspark.sql import functions as F
        first_ats_record = {
            'client_request_host': 'host',
            'client_ip': '55.555.55.55',
            '@timestamp': '2018-01-17T08:30:00.000Z',
            'geoip.location.lat': 51.5074,
            'geoip.location.lon': 0.1278,
        }
        second_ats_record = {
            'client_request_host': 'host',
            'client_ip': '55.555.55.55',
            '@timestamp': '2018-01-17T10:30:00.000Z',
            'geoip.location.lat': 51.5074,
            'geoip.location.lon': 0.1278,
        }

        sub_df = self.get_sub_df_for_feature(
            self.feature,
            [
                first_ats_record,
                second_ats_record,
            ]
        )
        result = self.feature.compute(sub_df)

        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(8.5).cast('float')
        )

        for k, v in self.feature.columns_renamed.items():
            expected_df = expected_df.withColumnRenamed(k, v)
        expected_df = self.schema_helper(
            expected_df, result.schema, [self.feature.feature_name]
        )
        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_update_row(self):
        requests = FeatureRequestTotal()
        test_current = {self.feature.feature_name: 6.,
                        requests.feature_name: 3.}
        test_past = {self.feature.feature_name: 2.,
                     requests.feature_name: 1.}
        value = self.feature.update_row(
            test_current, test_past
        )

        expected_value = 0.75*6. + 0.25*2.
        self.assertAlmostEqual(value, expected_value, places=2)

    def test_update(self):
        schema = T.StructType([
            T.StructField(
                self.feature.current_features_column,
                T.MapType(T.StringType(), T.FloatType())
            ),
            T.StructField(
                self.feature.past_features_column,
                T.MapType(T.StringType(), T.FloatType())
            ),
        ])

        sub_df = self.session.createDataFrame(
            [{
                self.feature.current_features_column: {
                    self.feature.feature_name: 6.,
                    FeatureRequestTotal.feature_name_from_class(): 3.,
                },
                self.feature.past_features_column: {
                    self.feature.feature_name: 2.,
                    FeatureRequestTotal.feature_name_from_class(): 1.,
                }
            }],
            schema=schema
        )
        result_df = self.feature.update(
            sub_df
        )

        result_df.show()
        value = result_df.select(self.feature.updated_feature_col_name).collect()[0][self.feature.updated_feature_col_name]
        expected_value = 0.75 * 6. + 0.25 * 2.
        self.assertAlmostEqual(value, expected_value, places=2)

