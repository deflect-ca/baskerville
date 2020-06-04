from baskerville.features.feature_minutes_total import FeatureMinutesTotal
from baskerville.util.enums import FeatureComputeType
from pyspark.sql import functions as F, types as T

from tests.unit.baskerville_tests.helpers.spark_testing_base import \
    FeatureSparkTestCase


class TestSparkMinutesTotal(FeatureSparkTestCase):

    def setUp(self):
        super(TestSparkMinutesTotal, self).setUp()
        self.feature = FeatureMinutesTotal()

    def test_instance(self):
        self.assertTrue(hasattr(self.feature, 'feature_name'))
        self.assertTrue(hasattr(self.feature, 'COLUMNS'))
        self.assertTrue(hasattr(self.feature, 'DEPENDENCIES'))
        self.assertTrue(hasattr(self.feature, 'DEFAULT_VALUE'))
        self.assertTrue(hasattr(self.feature, 'compute_type'))

        self.assertTrue(self.feature.feature_name == 'minutes_total')
        self.assertTrue(self.feature.columns == ['@timestamp'])
        self.assertTrue(self.feature.dependencies == [])
        self.assertTrue(self.feature.DEFAULT_VALUE == 0.)
        self.assertTrue(self.feature.compute_type ==
                        FeatureComputeType.replace)
        self.assertIsNotNone(self.feature.feature_name)
        self.assertIsNotNone(self.feature.feature_default)

        self.assertTrue(isinstance(self.feature.feature_name, str))
        self.assertTrue(isinstance(self.feature.feature_default, float))

    def test_compute_single_record_first_subset(self):
        ats_record = {
            "client_ip": '55.555.55.55',
            "client_request_host": 'host',
            "@timestamp": '2018-01-17T08:30:00.000Z',
            "content_type": 'application/javascript',
            "client_url": 'page1/page2/page3?query',
        }
        sub_df = self.get_df_with_extra_cols(
            self.feature,
            [ats_record],
            extra_cols={
                'first_ever_request': F.lit(None).cast(
                    'timestamp')
            }
        )
        result = self.feature.compute(sub_df)
        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(0.).cast('float')
        )
        expected_df = self.schema_helper(
            expected_df, result.schema, [self.feature.feature_name]
        )

        result.show()
        expected_df.show()

        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_compute_single_record_subsequent_subset(self):
        from pyspark.sql import functions as F
        ats_record = {
            "client_ip": '55.555.55.55',
            "client_request_host": 'host',
            "@timestamp": '2018-01-17T08:40:00.000Z',
            "content_type": 'application/javascript',
            "client_url": 'page1/page2/page3?query',
        }
        first_ever_request = '2018-01-17T08:30:00.000Z'
        sub_df = self.get_df_with_extra_cols(
            self.feature,
            [ats_record],
            extra_cols={
                'first_ever_request': F.lit(first_ever_request).cast(
                    'timestamp')
            }
        )
        result = self.feature.compute(sub_df)
        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(10.).cast('float')
        )
        expected_df = self.schema_helper(
            expected_df, result.schema, [self.feature.feature_name]
        )

        result.show()
        expected_df.show()

        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_compute_multiple_records_first_subset(self):
        from pyspark.sql import functions as F
        first_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            "@timestamp": '2018-01-17T08:35:00.000Z',
            'agent': 'ua',
            'content_type': 'application/javascript',
            'request': '/one/two/three/four.png',
        }
        second_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            "@timestamp": '2018-01-17T08:35:00.000Z',
            'agent': 'ua',
            'content_type': 'application/javascript',
            'request': '/one/two/three.png',
        }
        third_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            "@timestamp": '2018-01-17T08:50:00.000Z',
            'agent': 'ua',
            'content_type': 'html',
            'request': '/one/two.png',
        }

        sub_df = self.get_df_with_extra_cols(
            self.feature,
            [first_ats_record, second_ats_record, third_ats_record],
            extra_cols={
                'first_ever_request': F.lit(None).cast(
                    'timestamp')
            }
        )
        result = self.feature.compute(sub_df)

        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(15.).cast('float')
        )
        expected_df = self.schema_helper(
            expected_df, result.schema, [self.feature.feature_name]
        )

        result.show()
        expected_df.show()

        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_compute_multiple_records_subsequent_subset(self):
        from pyspark.sql import functions as F
        first_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            "first_ever_request": '2018-01-17T08:30:00.000Z',
            "@timestamp": '2018-01-17T08:35:00.000Z',
            'agent': 'ua',
            'content_type': 'application/javascript',
            'request': '/one/two/three/four.png',
        }
        second_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            "first_ever_request": '2018-01-17T08:30:00.000Z',
            "@timestamp": '2018-01-17T08:35:00.000Z',
            'agent': 'ua',
            'content_type': 'application/javascript',
            'request': '/one/two/three.png',
        }
        third_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            "first_ever_request": '2018-01-17T08:30:00.000Z',
            "@timestamp": '2018-01-17T08:50:00.000Z',
            'agent': 'ua',
            'content_type': 'html',
            'request': '/one/two.png',
        }
        first_ever_request = '2018-01-17T08:30:00.000Z'

        sub_df = self.get_df_with_extra_cols(
            self.feature,
            [first_ats_record, second_ats_record, third_ats_record],
            extra_cols={
                'first_ever_request': F.lit(first_ever_request).cast(
                    'timestamp')
            }
        )

        result = self.feature.compute(sub_df)

        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(20.).cast('float')
        )
        expected_df = self.schema_helper(
            expected_df, result.schema, [self.feature.feature_name]
        )

        result.show()
        expected_df.show()

        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_update_row(self):
        test_current = {self.feature.feature_name: 2.}
        test_past = {self.feature.feature_name: 1.}
        value = self.feature.update_row(
            test_current, test_past
        )

        self.assertAlmostEqual(value, 2., places=2)

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
                    self.feature.feature_name: 2.,
                },
                self.feature.past_features_column: {
                    self.feature.feature_name: 1.,
                }
            }],
            schema=schema
        )
        result_df = self.feature.update(
            sub_df
        )

        result_df.show()
        value = result_df.select(
            self.feature.updated_feature_col_name
        ).collect()[0][self.feature.updated_feature_col_name]
        expected_value = 2.
        self.assertAlmostEqual(value, expected_value, places=2)
