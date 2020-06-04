import os
import unittest
from datetime import datetime

import pyspark
from baskerville.util.helpers import parse_config, periods_overlap, \
    instantiate_from_str, class_from_str


class TestHelpers(unittest.TestCase):
    def setUp(self):
        self.test_file_name = f'{os.path.abspath(".")}/testfile.yaml'

    def tearDown(self):
        if os.path.isfile(self.test_file_name):
            os.remove(self.test_file_name)

    def test_parse_config_with_data(self):
        os.environ['TEST_ENV_TAG'] = 'it works!'
        os.environ['OTHER_TEST_TAG'] = 'this works too!'
        test_data = '''
        test1:
            data0: !ENV ${TEST_ENV_TAG}
            data1:  !ENV ${OTHER_TEST_TAG}
        '''
        config = parse_config(data=test_data)

        expected_config = {
            'test1': {
                'data0': 'it works!',
                'data1': 'this works too!'
            }
        }

        self.assertDictEqual(
            config,
            expected_config
        )

    def test_parse_config_with_file_path(self):
        os.environ['TEST_ENV_TAG'] = 'it works!'
        os.environ['OTHER_TEST_TAG'] = 'this works too!'
        test_data = '''
        test1:
            data0: !ENV ${TEST_ENV_TAG}
            data1:  !ENV ${OTHER_TEST_TAG}
        '''
        with open(self.test_file_name, 'w') as test_file:
            test_file.write(test_data)

        config = parse_config(path=self.test_file_name)

        expected_config = {
            'test1': {
                'data0': 'it works!',
                'data1': 'this works too!'
            }
        }

        self.assertDictEqual(
            config,
            expected_config
        )

    def test_parse_config_diff_tag(self):
        os.environ['TEST_ENV_TAG'] = 'it works!'
        os.environ['OTHER_TEST_TAG'] = 'this works too!'
        test_data = '''
        test1:
            data0: !TEST ${TEST_ENV_TAG}
            data1:  !TEST ${OTHER_TEST_TAG}
        '''
        config = parse_config(data=test_data, tag='!TEST')

        expected_config = {
            'test1': {
                'data0': 'it works!',
                'data1': 'this works too!'
            }
        }

        self.assertDictEqual(
            config,
            expected_config
        )

    def test_parse_config_more_than_one_env_value(self):
        os.environ['TEST_ENV_TAG'] = 'it works!'
        os.environ['OTHER_TEST_TAG'] = 'this works too!'
        test_data = '''
        test1:
            data0: !TEST ${TEST_ENV_TAG}/somethingelse/${OTHER_TEST_TAG}
            data1:  !TEST ${OTHER_TEST_TAG}
        '''
        config = parse_config(data=test_data, tag='!TEST')

        expected_config = {
            'test1': {
                'data0': 'it works!/somethingelse/this works too!',
                'data1': 'this works too!'
            }
        }

        self.assertDictEqual(
            config,
            expected_config
        )

    def test_periods_overlap(self):
        start = datetime(2018, 1, 1, 10, 20, 00)
        end = datetime(2018, 1, 3, 10, 20, 00)
        other_start = datetime(2018, 1, 1, 10, 20, 00)
        other_end = datetime(2018, 1, 1, 23, 20, 00)

        overlap = periods_overlap(start, end, other_start, other_end)

        self.assertTrue(overlap)

    def test_periods_overlap_allow_closed_interval(self):
        start = datetime(2018, 1, 1, 10, 20, 00)
        end = datetime(2018, 1, 3, 10, 20, 00)
        other_start = datetime(2018, 1, 3, 10, 20, 00)
        other_end = datetime(2018, 1, 3, 23, 20, 00)

        overlap_allow_closed_interval = periods_overlap(
            start, end, other_start, other_end, allow_closed_interval=True
        )

        overlap_do_not_allow_closed_interval = periods_overlap(
            start, end, other_start, other_end, allow_closed_interval=False
        )

        self.assertTrue(overlap_allow_closed_interval)
        self.assertFalse(overlap_do_not_allow_closed_interval)

    def test_periods_overlap_not(self):
        start = datetime(2018, 1, 1, 10, 20, 00)
        end = datetime(2018, 1, 3, 10, 20, 00)
        other_start = datetime(2018, 1, 3, 10, 21, 00)
        other_end = datetime(2018, 1, 3, 23, 20, 00)

        overlap_allow_closed = periods_overlap(
            start, end, other_start, other_end, allow_closed_interval=True
        )

        overlap_not_closed = periods_overlap(
            start, end, other_start, other_end, allow_closed_interval=False
        )

        self.assertFalse(overlap_allow_closed)
        self.assertFalse(overlap_not_closed)

    def test_instantiate_from_str(self):
        instance = instantiate_from_str('pyspark.sql.DataFrame', None, None)
        self.assertTrue(isinstance(instance, pyspark.sql.DataFrame))

    def test_class_from_str(self):
        cls = class_from_str('pyspark.sql.DataFrame')
        self.assertTrue(cls, pyspark.sql.DataFrame)
