import numpy as np
from pyspark.sql import functions as F, types as T

from baskerville.features.feature_path_depth_variance import \
    FeaturePathDepthVariance
from tests.unit.baskerville_tests.helpers.spark_testing_base import \
    FeatureSparkTestCase
from baskerville.features.feature_request_total import FeatureRequestTotal
from baskerville.features.feature_path_depth_average import \
    FeaturePathDepthAverage
from baskerville.util.enums import FeatureComputeType


class TestSparkPathDepthVariance(FeatureSparkTestCase):

    def setUp(self):
        super(TestSparkPathDepthVariance, self).setUp()
        self.feature = FeaturePathDepthVariance()

    def test_instance(self):
        self.assertTrue(hasattr(self.feature, 'feature_name'))
        self.assertTrue(hasattr(self.feature, 'COLUMNS'))
        self.assertTrue(hasattr(self.feature, 'DEPENDENCIES'))
        self.assertTrue(hasattr(self.feature, 'DEFAULT_VALUE'))
        self.assertTrue(hasattr(self.feature, 'compute_type'))

        self.assertTrue(self.feature.feature_name == 'path_depth_variance')
        self.assertTrue(
            self.feature.columns == ['client_url'])
        self.assertTrue(self.feature.dependencies == [FeatureRequestTotal,
                                                      FeaturePathDepthAverage])
        self.assertTrue(self.feature.DEFAULT_VALUE == 0.)
        self.assertTrue(self.feature.compute_type == FeatureComputeType.variance)
        self.assertIsNotNone(self.feature.feature_name)
        self.assertIsNotNone(self.feature.feature_default)

        self.assertTrue(isinstance(self.feature.feature_name, str))
        self.assertTrue(isinstance(self.feature.feature_default, float))

    def test_compute_single_record(self):
        ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:30:00.000Z',
            "content_type": 'html',
            "client_url": 'page1/page2/page3?query',
        }
        sub_df = self.session.createDataFrame([ats_record])
        sub_df = sub_df.withColumn(
            '@timestamp', F.col('@timestamp').cast('timestamp')
        )
        for k, v in self.feature.pre_group_by_calcs.items():
            sub_df = sub_df.withColumn(k, v)

        sub_df = sub_df.groupby('client_ip').agg(
            *[v.alias(k) for k, v in self.feature.group_by_aggs.items()]
        )

        result = self.feature.compute(sub_df)
        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(self.feature.feature_default).cast('float')
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

    def test_compute_multiple_records(self):
        first_ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:30:00.000Z',
            "content_type": 'text/html',
            "client_url": 'page1/page2/page3',
        }
        second_ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:30:00.000Z',
            "content_type": 'application/javascript',
            "client_url": 'page1/page2',
        }
        third_ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:30:00.000Z',
            "content_type": 'text/html',
            "client_url": 'page1',
        }

        sub_df = self.session.createDataFrame(
            [first_ats_record, second_ats_record, third_ats_record]
        )

        sub_df = sub_df.withColumn(
            '@timestamp', F.col('@timestamp').cast('timestamp')
        )
        for k, v in self.feature.pre_group_by_calcs.items():
            sub_df = sub_df.withColumn(k, v)

        sub_df = sub_df.groupby('client_ip').agg(
            *[v.alias(k) for k, v in self.feature.group_by_aggs.items()]
        )

        # feature returns float
        result = self.feature.compute(sub_df)
        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(np.var([0., 1., 2.], ddof=1)).cast('float')
        )
        expected_df = self.schema_helper(
            expected_df,
            result.schema,
            [self.feature.feature_name]
        )

        result.show()
        expected_df.show()

        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_compute_multiple_records_different_depth(self):
        first_ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:30:00.000Z',
            "content_type": 'html',
            "client_url": 'page1/page2/page3',
        }
        second_ats_record = {
            "client_ip": '55.555.55.55',
            "@timestamp": '2018-01-17T08:30:00.000Z',
            "content_type": 'html',
            "client_url": 'page1/page2',
        }
        sub_df = self.session.createDataFrame(
            [first_ats_record, second_ats_record]
        )

        sub_df = sub_df.withColumn(
            '@timestamp', F.col('@timestamp').cast('timestamp')
        )
        for k, v in self.feature.pre_group_by_calcs.items():
            sub_df = sub_df.withColumn(k, v)

        sub_df = sub_df.groupby('client_ip').agg(
            *[v.alias(k) for k, v in self.feature.group_by_aggs.items()]
        )

        # feature returns float
        result = self.feature.compute(sub_df)
        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(np.var([1., 2.], ddof=1)).cast('float')
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
        requests = FeatureRequestTotal()
        path_depth_ave = FeaturePathDepthAverage()
        test_current = {self.feature.feature_name: 6.,
                        requests.feature_name: 3.,
                        path_depth_ave.feature_name: 5.}
        test_past = {self.feature.feature_name: 2.,
                     requests.feature_name: 1.,
                     path_depth_ave.feature_name: 4.}
        value = self.feature.update_row(
            test_current, test_past
        )

        from baskerville.features.helpers import update_variance
        expected_value = update_variance(2., 6., 1., 3., 4., 5.)

        self.assertAlmostEqual(value, expected_value, places=2)

    def test_update(self):
        count_col = FeatureRequestTotal.feature_name_from_class()
        mean_col = FeaturePathDepthAverage.feature_name_from_class()
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
                    count_col: 3.,
                    mean_col: 5.,
                },
                self.feature.past_features_column: {
                    self.feature.feature_name: 2.,
                    count_col: 1.,
                    mean_col: 4.,
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
        from baskerville.features.helpers import update_variance
        expected_value = update_variance(2., 6., 1., 3., 4., 5.)
        print(expected_value)
        self.assertAlmostEqual(value, expected_value, places=2)
