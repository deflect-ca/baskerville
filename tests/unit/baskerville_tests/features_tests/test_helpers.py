import unittest
from unittest import mock

from baskerville.features.helpers import update_features, \
    extract_features_in_order


class TestHelpers(unittest.TestCase):
    def test_update_mean(self):
        pass

    def test_update_variance(self):
        pass

    def test_update_total(self):
        pass

    def test_update_rate(self):
        pass

    def test_update_maximum(self):
        pass

    def test_update_minimum(self):
        pass

    def test_update_ratio(self):
        pass

    def test_update_replace(self):
        pass

    def test_update_features(self):
        features_cur = {
            'f1': 1.0,
            'f2': 0.5
        }
        features_old = {}
        subset_count = 2
        start = None
        last_request = None

        f1 = mock.MagicMock()
        f2 = mock.MagicMock()
        f1.feature_name = 'f1'
        f2.feature_name = 'f2'
        f1.update_row.return_value = 100
        f2.update_row.return_value = 220

        active_features = [f1, f2]

        actual_value = update_features(
            active_features,
            features_cur,
            features_old,
            subset_count,
            start,
            last_request
        )
        expected_value = {
            'f1': 100,
            'f2': 220,
        }

        self.assertDictEqual(actual_value, expected_value)

    def test_update_features_diff(self):

        features_old = {'f1': 0, 'f2': 1, 'f3': 3, 'f5': 10}
        features_cur = {'f1': 1, 'f3': 5, 'f4': 0, 'f5': 2}
        subset_count = 1
        start = None
        last_request = None

        f1 = mock.MagicMock()
        f1.feature_name = 'f1'
        f1.update_row.return_value = 1.0
        f2 = mock.MagicMock()
        f2.feature_name = 'f2'
        f2.update_row.return_value = 2.0
        f3 = mock.MagicMock()
        f3.feature_name = 'f3'
        f3.update_row.return_value = 3.0
        f4 = mock.MagicMock()
        f4.feature_name = 'f4'
        f4.update_row.return_value = 4.0
        f5 = mock.MagicMock()
        f5.feature_name = 'f5'
        f5.update_row.return_value = 5.0

        active_features = [f1, f3, f4, f5]

        features_new = update_features(active_features,
                                       features_cur,
                                       features_old,
                                       subset_count=subset_count,
                                       start=start,
                                       last_request=last_request)
        self.assertDictEqual(
            features_new, {'f1': 1.0, 'f3': 3.0, 'f4': 4.0, 'f5': 5.0}
        )
        f1.update_row.assert_called_once_with(
            features_cur,
            features_old,
            subset_count=subset_count,
            start=start,
            last_request=last_request
        )
        f2.update_row.assert_not_called()
        f3.update_row.assert_called_once_with(
            features_cur,
            features_old,
            subset_count=subset_count,
            start=start,
            last_request=last_request
        )
        f4.update_row.assert_called_once_with(
            features_cur,
            features_old,
            subset_count=subset_count,
            start=start,
            last_request=last_request
        )
        f5.update_row.assert_called_once_with(
            features_cur,
            features_old,
            subset_count=subset_count,
            start=start,
            last_request=last_request
        )

    def test_extract_features_in_order(self):
        feature_dict = {'f1': 1, 'f2': 2, 'f4': 4, 'f3': 3}
        model_features = ['f1', 'f3', 'f2']
        ordered_feature_vals = extract_features_in_order(feature_dict,
                                                         model_features)

        self.assertEqual(ordered_feature_vals,
                         [1, 3, 2])
