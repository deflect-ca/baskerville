import unittest

from baskerville.models.stats import PerformanceStatsRegistry, Stats


class TestStats(unittest.TestCase):
    def setUp(self):
        self.stats = Stats()

    def tearDown(self):
        if hasattr(f_test, '__self__'):
            delattr(f_test, '__self__')

    def test_instance(self):
        self.assertTrue(hasattr(self.stats, '_prefix'))
        self.assertTrue(hasattr(self.stats, '_Stats__registry'))

    def test_prefix(self):
        name = 'test_name'
        expected = f'{self.stats._prefix}{name}'
        actual = self.stats.prefix(name)
        self.assertEqual(actual, expected)

    def test_get_parent_name_no__self__(self):
        expected_parent_name = ''
        actual_parent_name = self.stats.get_parent_name(f_test)
        self.assertEqual(actual_parent_name, expected_parent_name)

    def test_get_parent_name(self):
        f_test.__self__ = self
        expected_parent_name = self.__class__.__name__
        actual_parent_name = self.stats.get_parent_name(f_test)
        self.assertEqual(actual_parent_name, expected_parent_name)

    def test_get_name(self):
        name = 'test_name'
        expected_without_name = f'{self.stats._prefix}{f_test.__name__}'
        expected_with_name = f'{self.stats._prefix}{name}'
        actual_without_name = self.stats.get_name(f_test)
        actual_with_name = self.stats.get_name(f_test, name=name)

        self.assertEqual(actual_without_name, expected_without_name)
        self.assertEqual(actual_with_name, expected_with_name)

    def test_get_description(self):
        name = 'test_name'
        expected_descr = f'Stats for {name} "{f_test.__name__}"'
        actual_descr = self.stats.get_description(f_test, name)
        self.assertEqual(actual_descr, expected_descr)


class TestPerformanceStatsRegistry(unittest.TestCase):
    def setUp(self):
        self.stats = PerformanceStatsRegistry()

    def test_instance(self):
        self.assertTrue(hasattr(self.stats, '_prefix'))
        self.assertTrue(hasattr(self.stats, '_Stats__registry'))

    def test_get_description(self):
        name = 'test_name'
        expected_descr = f'Time Performance of {name} "{f_test.__name__}"'
        actual_descr = self.stats.get_description(f_test, name)
        self.assertEqual(actual_descr, expected_descr)

    def test_register(self):
        wrapped_f = self.stats.register(f_test)
        expected_name = f'{self.stats._prefix}{f_test.__name__}'
        self.assertEqual(wrapped_f.__name__, f_test.__name__)
        # self.assertTrue(
        #     isinstance(self.stats.metrics[expected_name], (Summary,))
        # )
        # the above won't work because prometheus_client wraps the Summary
        # class
        self.assertEqual(
            self.stats.registry[expected_name].__class__.__name__,
            'Summary'
        )


class TestProgressStatsRegistry(unittest.TestCase):
    def setUp(self):
        self.stats = PerformanceStatsRegistry()

    def test_instance(self):
        self.assertTrue(hasattr(self.stats, '_prefix'))
        self.assertTrue(hasattr(self.stats, '_Stats__registry'))

    # def test_get_description(self):
    #     raise NotImplementedError()
    #
    # def test_before_wrapper(self):
    #     raise NotImplementedError()
    #
    # def test_after_wrapper(self):
    #     raise NotImplementedError()
    #
    # def test_register_action_hook(self):
    #     raise NotImplementedError()
    #
    # def test_register(self):
    #     raise NotImplementedError()


def f_test():
    pass
