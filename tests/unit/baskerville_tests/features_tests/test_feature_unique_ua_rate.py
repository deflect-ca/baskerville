from pyspark.sql import functions as F, types as T

from baskerville.util.enums import FeatureComputeType
from baskerville.features.feature_unique_ua_rate import \
    FeatureUniqueUaRate, FeatureMinutesTotal, FeatureUniqueUaTotal

from tests.unit.baskerville_tests.helpers.spark_testing_base import \
    FeatureSparkTestCase


class TestSparkUniqueUaRate(FeatureSparkTestCase):

    def setUp(self):
        super(TestSparkUniqueUaRate, self).setUp()
        self.feature = FeatureUniqueUaRate()

    def test_instance(self):
        self.assertTrue(hasattr(self.feature, 'feature_name'))
        self.assertTrue(hasattr(self.feature, 'COLUMNS'))
        self.assertTrue(hasattr(self.feature, 'DEPENDENCIES'))
        self.assertTrue(hasattr(self.feature, 'DEFAULT_VALUE'))
        self.assertTrue(hasattr(self.feature, 'compute_type'))

        self.assertTrue(self.feature.feature_name == 'unique_ua_rate')
        self.assertTrue(
            self.feature.columns == ['client_ua', '@timestamp'])
        self.assertTrue(self.feature.dependencies == [FeatureMinutesTotal,
                                                      FeatureUniqueUaTotal])
        self.assertTrue(self.feature.DEFAULT_VALUE == 1.)
        self.assertTrue(self.feature.compute_type == FeatureComputeType.rate)
        self.assertIsNotNone(self.feature.feature_name)
        self.assertIsNotNone(self.feature.feature_default)

        self.assertTrue(isinstance(self.feature.feature_name, str))
        self.assertTrue(isinstance(self.feature.feature_default, float))

    def test_compute_single_record_first_subset(self):
        from pyspark.sql import functions as F
        ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:30:00.000Z',
            "content_type": 'html',
            "client_url": 'page1/page2/page3?ua',
            "client_ua": ''
        }
        sub_df = self.get_df_with_extra_cols(
            self.feature,
            [ats_record],
            extra_cols={
                'first_ever_request': F.lit(None).cast('timestamp')
            }
        )
        result = self.feature.compute(sub_df)
        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(self.feature.feature_default).cast('float')
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
            "@timestamp": '2018-01-17T08:40:00.000Z',
            "first_ever_request": '2018-01-17T08:30:00.000Z',
            "content_type": 'html',
            "client_url": 'page1/page2/page3?ua',
            "client_ua": ''
        }
        sub_df = self.get_df_with_extra_cols(
            self.feature, [ats_record], extra_cols={
                'first_ever_request': F.lit('2018-01-17T08:30:00.000Z').cast(
                    'timestamp')
            })
        result = self.feature.compute(sub_df)
        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(1./10.).cast('float')
        )

        result.show()
        expected_df.show()

        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_compute_different_uas_first_subset(self):
        from pyspark.sql import functions as F
        first_ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:30:00.000Z',
            "content_type": 'html',
            "client_url": 'page1/page2',
            "client_ua": 'ua1',
        }
        second_ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:40:00.000Z',
            "content_type": 'html',
            "client_url": 'page1/page2',
            "client_ua": 'ua1',
        }
        third_ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:50:00.000Z',
            "content_type": 'html',
            "client_url": 'page1/page2',
            "client_ua": 'ua2',
        }

        sub_df = self.get_df_with_extra_cols(
            self.feature,
            [
                first_ats_record,
                second_ats_record,
                third_ats_record,
            ],
            extra_cols = {
                'first_ever_request': F.lit(None).cast('timestamp')
            }
        )
        result = self.feature.compute(sub_df)

        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(2./20.).cast('float')
        )

        result.show()
        expected_df.show()

        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_compute_ddifferent_queries_subsequent_subset(self):
        from pyspark.sql import functions as F
        first_ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:45:00.000Z',
            "content_type": 'html',
            "client_url": 'page1/page2',
            "client_ua": 'ua1',
        }
        second_ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:40:00.000Z',
            "content_type": 'html',
            "client_url": 'page1/page2',
            "client_ua": 'ua2',
        }
        third_ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:50:00.000Z',
            "content_type": 'html',
            "client_url": 'page1/page2',
            "client_ua": 'ua3',
        }

        sub_df = self.get_df_with_extra_cols(
            self.feature,
            [
                first_ats_record,
                second_ats_record,
                third_ats_record,
            ],
            extra_cols = {
                'first_ever_request': F.lit('2018-01-17T08:30:00.000Z').cast(
                    'timestamp')
            }
        )
        result = self.feature.compute(sub_df)

        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(3. / 20.).cast('float')
        )

        result.show()
        expected_df.show()

        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_update_row(self):
        total = FeatureUniqueUaTotal()
        minutes = FeatureMinutesTotal()
        test_current = {total.feature_name: 6.,
                        minutes.feature_name: 3.}
        test_past = {total.feature_name: 2.,
                     minutes.feature_name: 1.}
        value = self.feature.update_row(
            test_current, test_past
        )

        expected_value = (6. + 2.)/3.
        self.assertAlmostEqual(value, expected_value, places=2)

    def test_update(self):
        denominator = FeatureMinutesTotal.feature_name_from_class()
        numerator = FeatureUniqueUaTotal.feature_name_from_class()
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
                    numerator: 6.,
                    denominator: 3.,
                },
                self.feature.past_features_column: {
                    numerator: 2.,
                    denominator: 1.,
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
        expected_value = (6. + 2.)/3.
        self.assertAlmostEqual(value, expected_value, places=2)
