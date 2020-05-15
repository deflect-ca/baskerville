from pyspark.sql import functions as F, types as T

from baskerville.features.feature_css_to_html_ratio import FeatureCssToHtmlRatio
from baskerville.util.enums import FeatureComputeType

from tests.unit.baskerville_tests.helpers.spark_testing_base import \
    FeatureSparkTestCase
from baskerville.features.feature_html_total import FeatureHtmlTotal
from baskerville.features.feature_css_total import FeatureCssTotal


class TestSparkCssToHtmlRatio(FeatureSparkTestCase):

    def setUp(self):
        super(TestSparkCssToHtmlRatio, self).setUp()
        self.feature = FeatureCssToHtmlRatio()

    def test_instance(self):
        self.assertTrue(hasattr(self.feature, 'feature_name'))
        self.assertTrue(hasattr(self.feature, 'COLUMNS'))
        self.assertTrue(hasattr(self.feature, 'DEPENDENCIES'))
        self.assertTrue(hasattr(self.feature, 'DEFAULT_VALUE'))
        self.assertTrue(hasattr(self.feature, 'compute_type'))

        self.assertTrue(self.feature.feature_name == 'css_to_html_ratio')
        self.assertTrue(self.feature.columns == ['content_type'])
        self.assertTrue(self.feature.dependencies == [FeatureCssTotal,
                                                      FeatureHtmlTotal])
        self.assertTrue(self.feature.DEFAULT_VALUE == 0.)
        self.assertTrue(self.feature.COMPUTE_TYPE == FeatureComputeType.ratio)
        self.assertIsNotNone(self.feature.feature_name)
        self.assertIsNotNone(self.feature.feature_default)

        self.assertTrue(isinstance(self.feature.feature_name, str))
        self.assertTrue(isinstance(self.feature.feature_default, float))

    def test_compute_single_record(self):
        ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:30:00.000Z',
            "content_type": 'text/css',
            "client_url": 'page1/page2/page3?query',
        }
        sub_df = self.get_sub_df_for_feature(self.feature, [ats_record])
        result = self.feature.compute(sub_df)
        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(1. / 0.01).cast('float')
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

    def test_compute_multiple_records_only_css(self):
        first_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            '@timestamp': '2018-04-17T08:00:00Z',
            'agent': 'ua',
            'content_type': 'text/css',
            'request': '/one/two/three/four.png',
        }
        second_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            '@timestamp': '2018-04-17T08:10:00Z',
            'agent': 'ua',
            'content_type': 'text/css',
            'request': '/one/two/three.png',
        }
        third_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            '@timestamp': '2018-04-17T09:00:00Z',
            'agent': 'ua',
            'content_type': 'text/css',
            'request': '/one/two.png',
        }

        sub_df = self.get_sub_df_for_feature(
            self.feature,
            [
                first_ats_record,
                second_ats_record,
                third_ats_record,
            ]
        )
        result = self.feature.compute(sub_df)

        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(3. / 0.01).cast('float')
        )
        expected_df = self.schema_helper(
            expected_df, result.schema, [self.feature.feature_name]
        )
        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_compute_multiple_records_no_css_no_html(self):
        first_ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:30:00.000Z',
            "content_type": 'other',
            "client_url": 'page1/page2/page3',
        }
        second_ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:30:00.000Z',
            "content_type": 'other2',
            "client_url": 'page1/page2',
        }
        third_ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:30:00.000Z',
            "content_type": 'other3',
            "client_url": 'page1/page2/page3',
        }

        sub_df = self.get_sub_df_for_feature(
            self.feature,
            [
                first_ats_record,
                second_ats_record,
                third_ats_record,
             ]
        )
        result = self.feature.compute(sub_df)

        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(0.).cast('float')
        )
        expected_df = self.schema_helper(
            expected_df, result.schema, [self.feature.feature_name]
        )
        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_compute_multiple_records_diff_type(self):
        first_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            '@timestamp': '2018-04-17T08:00:00Z',
            'agent': 'ua',
            'content_type': 'text/css',
            'request': '/one/two/three/four.png',
        }
        second_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            '@timestamp': '2018-04-17T08:10:00Z',
            'agent': 'ua',
            'content_type': 'text/css',
            'request': '/one/two/three.png',
        }
        third_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            '@timestamp': '2018-04-17T09:00:00Z',
            'agent': 'ua',
            'content_type': 'text/html',
            'request': '/one/two.png',
        }

        sub_df = self.get_sub_df_for_feature(
            self.feature,
            [
                first_ats_record,
                second_ats_record,
                third_ats_record,
             ]
        )
        result = self.feature.compute(sub_df)

        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(2.).cast('float')
        )
        expected_df = self.schema_helper(
            expected_df, result.schema, [self.feature.feature_name]
        )
        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_update_row(self):
        denominator = FeatureHtmlTotal()
        numerator = FeatureCssTotal()
        test_current = {self.feature.feature_name: 1.,
                        denominator.feature_name: 1.,
                        numerator.feature_name: 2.}
        test_past = {self.feature.feature_name: 1.,
                     denominator.feature_name: 2.,
                     numerator.feature_name: 4.}
        value = self.feature.update_row(
            test_current, test_past
        )

        self.assertAlmostEqual(value, 2., places=2)

    def test_update(self):
        denominator = FeatureHtmlTotal.feature_name_from_class()
        numerator = FeatureCssTotal.feature_name_from_class()
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
                    self.feature.feature_name: 1.,
                    numerator: 2.,
                    denominator: 1.,
                },
                self.feature.past_features_column: {
                    self.feature.feature_name: 1.,
                    numerator: 4.,
                    denominator: 2.,
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
