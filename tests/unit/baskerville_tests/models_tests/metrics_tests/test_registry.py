import unittest
from unittest import mock

from baskerville.models.metrics.registry import MetricsRegistry, StatsHook


def test_fn():
    return 42


class TestMetricsRegistry(unittest.TestCase):
    def tearDown(self) -> None:
        if hasattr(test_fn, '__self__'):
            del test_fn.__self__

    def test_singleton(self):
        mr = MetricsRegistry()
        mr2 = MetricsRegistry()
        self.assertTrue(mr == mr2)

    def test_prefix(self):
        mr = MetricsRegistry()
        text = 'test'
        actual = mr.prefix(text)
        expected = f'{mr._prefix}{text}'
        self.assertEqual(actual, expected)

    def test_get_parent_name(self):
        mr = MetricsRegistry()

        actual = mr.get_parent_name(test_fn)
        expected = ''
        self.assertEqual(actual, expected)

        test_fn.__self__ = {}

        actual = mr.get_parent_name(test_fn)
        expected = {}.__class__.__name__
        self.assertEqual(actual, expected)

    def test_get_name(self):
        mr = MetricsRegistry()
        actual = mr.get_name(test_fn)
        expected = f'{mr._prefix}{test_fn.__name__}'
        self.assertEqual(actual, expected)

        test_fn.__self__ = {}
        actual = mr.get_name(test_fn)
        expected = f'{mr._prefix}{dict.__name__}_{test_fn.__name__}'
        self.assertEqual(actual, expected)

        name = 'some_name'
        actual = mr.get_name(test_fn, name=name)
        expected = f'{mr._prefix}{name}'
        self.assertEqual(actual, expected)

    def test_name_integrity_check(self):
        mr = MetricsRegistry()
        name = 'some_name'
        actual = mr.name_integrity_check(test_fn)
        expected = f'{mr._prefix}{test_fn.__name__}'
        self.assertEqual(actual, expected)

        test_fn.__self__ = {}
        actual = mr.name_integrity_check(test_fn)
        expected = f'{mr._prefix}{dict.__name__}_{test_fn.__name__}'
        self.assertEqual(actual, expected)

        actual = mr.name_integrity_check(test_fn, name)
        expected = f'{mr._prefix}{name}'
        self.assertEqual(actual, expected)

    def test_name_integrity_check_duplicate_name(self):
        mr = MetricsRegistry()
        name = 'some_name'
        mr.registry[f'{mr._prefix}{name}'] = 'tada'
        with self.assertRaises(ValueError):
            mr.name_integrity_check(test_fn, name)

    def test_before_wrapper(self):
        mr = MetricsRegistry()
        name = 'some_name'
        test_fn.__self__ = {}

        mock_metric = mock.MagicMock()
        mr.registry[name] = StatsHook(
            metric=mock_metric,
            wrapped_func_name=name,
            hooks_to_callbacks={'before': lambda m, s: print(m, s)}
        )

        wrapped_fn = mr.before_wrapper(test_fn, name)
        self.assertTrue(hasattr(wrapped_fn, '__self__'))
        self.assertTrue(isinstance(wrapped_fn.__self__, dict))
        actual = wrapped_fn()
        expected = 42

        self.assertTrue(actual, expected)

    def test_after_wrapper(self):
        mr = MetricsRegistry()
        name = 'some_name'
        test_fn.__self__ = {}

        mock_metric = mock.MagicMock()
        mr.registry[name] = StatsHook(
            metric=mock_metric,
            wrapped_func_name=name,
            hooks_to_callbacks={'after': lambda m, s: print(m, s)}
        )

        wrapped_fn = mr.after_wrapper(test_fn, name)
        self.assertTrue(hasattr(wrapped_fn, '__self__'))
        self.assertTrue(isinstance(wrapped_fn.__self__, dict))
        actual = wrapped_fn()
        expected = 42

        self.assertTrue(actual, expected)

    def test_with_wrapper(self):
        mr = MetricsRegistry()
        name = 'some_name'
        test_fn.__self__ = {}

        mock_metric = mock.MagicMock()
        mr.registry[name] = mock_metric
        wrapped_fn = mr.with_wrapper(test_fn, name, kind=None, method='test')
        actual = wrapped_fn()
        expected = 42

        self.assertTrue(actual, expected)
        mock_metric.test.assert_called_once()

    def test_state_wrapper(self):
        mr = MetricsRegistry()
        name = 'some_name'
        test_state = 'test_state'
        test_fn.__self__ = {}
        mr._MetricsRegistry__func_name_to_state[test_fn.__name__] = test_state
        mock_metric = mock.MagicMock()
        mr.registry[name] = mock_metric
        wrapped_fn = mr.state_wrapper(test_fn, name)
        actual = wrapped_fn()
        expected = 42

        self.assertTrue(actual, expected)
        mock_metric.state.assert_called_once_with(test_state)
