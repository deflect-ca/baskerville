import unittest
from baskerville.features.base_feature import BaseFeature


class TestBaseFeature(unittest.TestCase):
    def test_instance(self):
        feature = BaseFeature()
        self.assertTrue(hasattr(feature, 'feature_name'))
        self.assertTrue(hasattr(feature, 'COLUMNS'))
        self.assertTrue(hasattr(feature, 'DEPENDENCIES'))
        self.assertTrue(hasattr(feature, 'DEFAULT_VALUE'))
        self.assertTrue(hasattr(feature, 'group_by_aggs'))
        self.assertTrue(hasattr(feature, 'pre_group_by_calcs'))
        self.assertTrue(hasattr(feature, 'columns_renamed'))

    def test_misc_compute(self):
        expected_value = {'the answer': 42}
        feature = BaseFeature()

        result = feature.misc_compute(expected_value)

        self.assertDictEqual(result, expected_value)
