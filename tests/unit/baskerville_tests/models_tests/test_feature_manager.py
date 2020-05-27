from unittest import mock
from pyspark.sql import functions as F
from baskerville.models.feature_manager import FeatureManager

from tests.unit.baskerville_tests.helpers.spark_testing_base import \
    SQLTestCaseLatestSpark
from tests.unit.baskerville_tests.helpers.utils import get_default_log_path


class TestFeatureManager(SQLTestCaseLatestSpark):
    def setUp(self):
        super(TestFeatureManager, self).setUp()
        self.db_tools_patcher = mock.patch(
            'baskerville.util.baskerville_tools.BaskervilleDBTools'
        )
        self.spark_patcher = mock.patch(
            'baskerville.models.base_spark.SparkPipelineBase.'
            'instantiate_spark_session'
        )
        self.mock_engine_conf = mock.MagicMock()
        self.mock_engine_conf.log_level = 'DEBUG'
        self.mock_engine_conf.logpath = get_default_log_path()

    def _helper_get_mock_features(self):
        mock_feature1 = mock.MagicMock()
        mock_feature2 = mock.MagicMock()
        mock_feature3 = mock.MagicMock()
        mock_feature4 = mock.MagicMock()
        return {
            'mock_feature1': mock_feature1,
            'mock_feature2': mock_feature2,
            'mock_feature3': mock_feature3,
            'mock_feature4': mock_feature4,
        }

    def _helper_check_features_instantiated_once(self, mock_features):
        for k, v in mock_features.items():
            v.assert_called_once()

    def test_initialize(self):
        mock_features = self._helper_get_mock_features()
        for k, f in mock_features.items():
            mock_features[k].DEPENDENCIES = []
            mock_features[k].return_value = mock.MagicMock(
                spec=[
                    'feature_name',
                    'columns',
                    'columns_renamed',
                    'pre_group_by_calcs',
                    'post_group_by_calcs',
                ]
            )
            mock_features[k].return_value.feature_name = k
            mock_features[k].return_value.columns = []
            mock_features[k].return_value.columns_renamed = {}
            mock_features[k].return_value.pre_group_by_calcs = {}
            mock_features[k].return_value.post_group_by_calcs = {}

        self.mock_engine_conf.extra_features = list(mock_features.keys())
        self.feature_manager = FeatureManager(
            self.mock_engine_conf
        )
        self.feature_manager.all_features = mock_features

        self.feature_manager.initialize()

        self.assertTrue(
            len(self.feature_manager.active_features),
            len(mock_features.values())
        )
        self.assertListEqual(
            sorted(self.feature_manager.active_feature_names),
            list(mock_features.keys())
        )
        self.assertListEqual(
            self.feature_manager.updateable_active_features, []
        )
        self.assertListEqual(
            self.feature_manager.update_feature_cols, []
        )
        self.assertListEqual(
            self.feature_manager.column_renamings, []
        )
        self.assertDictEqual(
            self.feature_manager.pre_group_by_calculations, {}
        )
        self.assertDictEqual(
            self.feature_manager.post_group_by_calculations, {}
        )

    def test_get_active_features_no_ml_model(self):
        mock_features = self._helper_get_mock_features()
        self.feature_manager = FeatureManager(
            self.mock_engine_conf
        )
        self.feature_manager.all_features = mock_features
        self.feature_manager.extra_features = mock_features.keys()

        actual_active_features = self.feature_manager.get_active_features()

        self._helper_check_features_instantiated_once(mock_features)
        self.assertEqual(
            len(actual_active_features),
            4
        )

    def test_get_active_features_no_ml_model_unknown_features(self):
        mock_features = self._helper_get_mock_features()
        self.feature_manager = FeatureManager(
            self.mock_engine_conf
        )
        self.feature_manager.all_features = mock_features
        self.feature_manager.extra_features = list(mock_features.keys()) + [
            'unknown_feature_1'
        ]

        # todo: with self.assertWarns(Warning):
        actual_active_features = self.feature_manager.get_active_features()

        self._helper_check_features_instantiated_once(mock_features)
        self.assertEqual(
            len(actual_active_features),
            4
        )

    def test_get_active_feature_names(self):
        mock_features = self._helper_get_mock_features()
        for k, f in mock_features.items():
            f.feature_name = k

        self.feature_manager = FeatureManager(self.mock_engine_conf)
        self.feature_manager.active_features = mock_features.values()

        feature_names = self.feature_manager.get_active_feature_names()

        self.assertEqual(feature_names, list(mock_features.keys()))

    def test_get_updateable_active_features(self):
        mock_features = self._helper_get_mock_features()
        for k, f in mock_features.items():
            f.updated_feature_col_name = k

        self.feature_manager = FeatureManager(self.mock_engine_conf)
        self.feature_manager.active_features = mock_features.values()

        feature_names = self.feature_manager.get_updateable_active_features()

        self.assertEqual(feature_names, list(mock_features.values()))

    def test_get_active_columns(self):
        mock_features = self._helper_get_mock_features()
        for k, f in mock_features.items():
            f.columns = ['test1', 'test2']

        self.feature_manager = FeatureManager(self.mock_engine_conf)
        self.feature_manager.active_features = mock_features.values()

        active_columns = self.feature_manager.get_active_columns()

        self.assertListEqual(sorted(active_columns), ['test1', 'test2'])

    def test_get_update_feature_columns(self):
        mock_features = self._helper_get_mock_features()
        for k, f in mock_features.items():
            f.updated_feature_col_name = k
            f.columns = ['test1', 'test2']

        self.feature_manager = FeatureManager(self.mock_engine_conf)
        self.feature_manager.active_features = mock_features.values()

        feature_names = self.feature_manager.get_updateable_active_features()

        self.assertEqual(feature_names, list(mock_features.values()))

    def test_get_update_feature_columns_no_updateable_features(self):
        mock_features = self._helper_get_mock_features()
        for k, f in mock_features.items():
            mock_features[k] = mock.MagicMock(spec=['columns'])
            mock_features[k].columns = ['test1', 'test2']
            self.assertFalse(
                hasattr(mock_features[k], 'updated_feature_col_name')
            )

        self.feature_manager = FeatureManager(self.mock_engine_conf)
        self.feature_manager.active_features = mock_features.values()

        feature_columns = self.feature_manager.get_update_feature_columns()

        self.assertEqual(feature_columns, [])

    def test_feature_config_is_valid_true(self):
        mock_features = self._helper_get_mock_features()
        for k, f in mock_features.items():
            f.feature_name_from_class.return_value = k
            f.dependencies = list(mock_features.values())

        self.feature_manager = FeatureManager(
            self.mock_engine_conf
        )
        self.feature_manager.active_features = mock_features.values()
        self.feature_manager.active_feature_names = list(mock_features.keys())

        is_config_valid = self.feature_manager.feature_config_is_valid()
        self.assertEqual(is_config_valid, True)

    def test_feature_config_is_valid_false_feature_dependencies_not_met(self):
        mock_features = self._helper_get_mock_features()
        for k, f in mock_features.items():
            f.feature_name_from_class.return_value = 'test'
            f.dependencies = list(mock_features.values())

        self.feature_manager = FeatureManager(
            self.mock_engine_conf
        )
        self.feature_manager.active_features = mock_features.values()
        self.feature_manager.active_feature_names = list(mock_features.keys())

        is_config_valid = self.feature_manager.feature_config_is_valid()
        self.assertEqual(is_config_valid, False)

    def test_feature_dependencies_met_true(self):
        mock_features = self._helper_get_mock_features()
        for k, f in mock_features.items():
            f.feature_name_from_class.return_value = k
            f.dependencies = list(mock_features.values())

        self.feature_manager = FeatureManager(
            self.mock_engine_conf
        )
        self.feature_manager.active_features = mock_features.values()
        self.feature_manager.active_feature_names = list(mock_features.keys())

        is_config_valid = self.feature_manager.feature_dependencies_met()
        self.assertEqual(is_config_valid, True)

    def test_feature_dependencies_met_false(self):
        mock_features = self._helper_get_mock_features()
        for k, f in mock_features.items():
            f.feature_name_from_class.return_value = 'other feature'
            f.dependencies = list(mock_features.values())

        self.feature_manager = FeatureManager(
            self.mock_engine_conf
        )
        self.feature_manager.active_features = mock_features.values()
        self.feature_manager.active_feature_names = list(mock_features.keys())

        is_config_valid = self.feature_manager.feature_dependencies_met()
        self.assertEqual(is_config_valid, False)

    def test_get_feature_pre_group_by_calcs(self):
        mock_features = self._helper_get_mock_features()
        for k, f in mock_features.items():
            f.pre_group_by_calcs = {'a': F.col('a')}
        feature_manager = FeatureManager(self.mock_engine_conf)
        feature_manager.active_features = mock_features.values()

        pre_group_by_calcs = feature_manager.get_feature_pre_group_by_calcs()
        self.assertEqual(len(pre_group_by_calcs), 1)
        self.assertEqual(list(pre_group_by_calcs.keys()), ['a'])
        self.assertColumnExprEqual(
            pre_group_by_calcs['a'], F.col('a').alias('a')
        )

    def test_get_feature_group_by_aggs(self):
        mock_features = self._helper_get_mock_features()
        for k, f in mock_features.items():
            f.group_by_aggs = {'a': F.col('a')}
        feature_manager = FeatureManager(self.mock_engine_conf)
        feature_manager.active_features = mock_features.values()

        group_by_aggs = feature_manager.get_feature_group_by_aggs()
        self.assertEqual(len(group_by_aggs), 1)
        self.assertEqual(list(group_by_aggs.keys()), ['a'])
        self.assertColumnExprEqual(
            group_by_aggs['a'], F.col('a').alias('a')
        )

    def test_get_feature_col_renamings(self):
        mock_features = self._helper_get_mock_features()
        for k, f in mock_features.items():
            f.columns_renamed = {'a': 'renamed_a'}
        feature_manager = FeatureManager(self.mock_engine_conf)
        feature_manager.active_features = mock_features.values()

        renamings = feature_manager.get_feature_col_renamings()
        self.assertEqual(len(renamings), 1)
        self.assertEqual(renamings, [('a', 'renamed_a')])

    def test_get_post_group_by_calculations(self):
        mock_features = self._helper_get_mock_features()
        for k, f in mock_features.items():
            f.post_group_by_calcs = {'a': F.col('a')}
        feature_manager = FeatureManager(self.mock_engine_conf)
        feature_manager.active_features = mock_features.values()

        post_group_by_aggs = feature_manager.get_post_group_by_calculations()
        self.assertEqual(len(post_group_by_aggs), 1)
        self.assertEqual(list(post_group_by_aggs.keys()), ['a'])
        self.assertColumnExprEqual(
            post_group_by_aggs['a'], F.col('a')
        )
