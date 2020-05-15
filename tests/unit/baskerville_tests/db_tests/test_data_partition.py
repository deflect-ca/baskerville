import unittest
from datetime import datetime, timedelta

import isoweek
from baskerville.db.data_partitioning import (
    DataPartitioner, TemporalDataPartitioner
)
from baskerville.db.temporal_partition import TimePeriod, \
    TemporalPartitionedTable
from baskerville.util.enums import PartitionByEnum

from tests.unit.baskerville_tests.helpers.utils import get_default_data_path


class TestDataPartitioner(unittest.TestCase):
    def setUp(self) -> None:
        self.parent_table = 'parent_table'
        self.partition_field = 'partition_field'
        self.index_by = 'index_by'
        self.template_path = get_default_data_path()
        self.template_name = 'sample_template.jinja2'
        self.instance = DataPartitioner(
            self.parent_table,
            self.partition_field,
            index_by=self.index_by,
            template_path=self.template_path,
            template_name=self.template_name
        )

    def test_instance(self):
        self.assertTrue(hasattr(self.instance, 'parent_table'))
        self.assertTrue(hasattr(self.instance, 'partition_field'))
        self.assertTrue(hasattr(self.instance, 'index_by'))
        self.assertTrue(hasattr(self.instance, 'template_path'))
        self.assertTrue(hasattr(self.instance, 'template_name'))

        self.assertEqual(self.instance.parent_table, self.parent_table)
        self.assertEqual(self.instance.partition_field, self.partition_field)
        self.assertEqual(self.instance.index_by, self.index_by)


class TestTemporalDataPartitioner(unittest.TestCase):
    def setUp(self) -> None:
        self.parent_table = 'parent_table'
        self.partition_field = 'partition_field'
        self.partition_by = PartitionByEnum.w
        self.index_by = list('abc')
        self.template_path = get_default_data_path()
        self.template_name = 'sample_template.jinja2'
        self.start = datetime(2019, 12, 1).replace(hour=0, minute=0, second=0)
        self.end = (self.start + timedelta(days=10))
        self.time_window = TimePeriod(self.start, self.end)

        self.instance = TemporalDataPartitioner(
            self.parent_table,
            self.partition_field,
            self.time_window,
            partition_by=self.partition_by,
            index_by=self.index_by,
            template_path=self.template_path,
            template_name=self.template_name
        )

    def test_instance(self):
        self.assertTrue(hasattr(self.instance, 'parent_table'))
        self.assertTrue(hasattr(self.instance, 'partition_field'))
        self.assertTrue(hasattr(self.instance, 'index_by'))
        self.assertTrue(hasattr(self.instance, 'template_path'))
        self.assertTrue(hasattr(self.instance, 'template_name'))

        self.assertEqual(self.instance.parent_table, self.parent_table)
        self.assertEqual(self.instance.partition_field, self.partition_field)
        self.assertEqual(self.instance.index_by, self.index_by)
        self.assertTrue(hasattr(self.instance, 'partitioned_table'))
        self.assertTrue(isinstance(
            self.instance.partitioned_table, TemporalPartitionedTable)
        )

    def test_to_dict_partition_by_month_not_strict(self):
        self.instance = TemporalDataPartitioner(
            self.parent_table,
            self.partition_field,
            self.time_window,
            partition_by=PartitionByEnum.m,
            index_by=self.index_by,
            template_path=self.template_path,
            template_name=self.template_name
        )
        results = self.instance.to_dict()
        print(results)
        f_start = self.start.strftime("%Y-%m-%d %H:%M:%S")
        f_end = self.end.replace(microsecond=999999).strftime("%Y-%m-%d %H:%M:%S.%f")
        expected_results = {
            'name': 'parent_table',
            'partition_prefix': 'parent_table_y2019_m',
            'partitions': [],
            'catch_all_partition_name': 'parent_table_catch_all',
            'partitioned_by': PartitionByEnum.m,
            'partition_field': 'partition_field',
            'field_value': f'cast(extract(month from NEW.{self.partition_field}) AS TEXT)',
            'self_check': f"NEW.{self.partition_field} >= '{f_start}' "
            f"AND NEW.{self.partition_field} <= '{f_end}' "
        }

        self.assertTrue(len(results.keys()) == len(expected_results.keys()))
        for k, v in results.items():
            if k != 'partitions':
                print(results[k])
                print(expected_results[k])
                self.assertTrue(results[k] == expected_results[k])

    def test_to_dict_partition_by_month_strict(self):
        self.start = datetime(2019, 12, 1)
        self.end = (
                self.start + timedelta(days=10)
        ).replace(microsecond=999999)
        self.time_window = TimePeriod(self.start, self.end)

        self.instance = TemporalDataPartitioner(
            self.parent_table,
            self.partition_field,
            self.time_window,
            partition_by=PartitionByEnum.m,
            index_by=self.index_by,
            template_path=self.template_path,
            template_name=self.template_name,
            strict=True
        )
        results = self.instance.to_dict()
        print(results)
        f_start = self.start.strftime("%Y-%m-%d %H:%M:%S")
        f_end = self.end.strftime("%Y-%m-%d %H:%M:%S.%f")
        expected_results = {
            'name': 'parent_table',
            'partition_prefix': 'parent_table_y2019_m',
            'partitions': [],
            'catch_all_partition_name': 'parent_table_catch_all',
            'partitioned_by': PartitionByEnum.m,
            'partition_field': 'partition_field',
            'field_value': f'cast(extract(month from NEW.{self.partition_field}) AS TEXT)',
            'self_check': f"NEW.{self.partition_field} >= '{f_start}' "
            f"AND NEW.{self.partition_field} <= '{f_end}' "
        }

        self.assertTrue(len(results.keys()) == len(expected_results.keys()))
        for k, v in results.items():
            if k != 'partitions':
                print(results[k])
                print(expected_results[k])
                self.assertTrue(results[k] == expected_results[k])

    def test_to_dict_partition_by_week_not_strict(self):
        results = self.instance.to_dict()
        start_w = isoweek.Week(self.start.year, self.start.isocalendar()[1])
        end_w = isoweek.Week(self.end.year, self.end.isocalendar()[1])
        f_start = datetime.combine(
            start_w.monday(), datetime.min.time()
        ).strftime("%Y-%m-%d %H:%M:%S")
        f_end = datetime.combine(
            end_w.sunday(), datetime.min.time()
        ).replace(
            hour=23, minute=59, second=59, microsecond=999999
        ).strftime("%Y-%m-%d %H:%M:%S.%f")

        print(f_start, f_end)
        expected_results = {
            'name': 'parent_table',
            'partition_prefix': 'parent_table_y2019_w',
            'partitions': [],
            'catch_all_partition_name': 'parent_table_catch_all',
            'partitioned_by': 'week', 'partition_field': 'partition_field',
            'field_value': f'cast(extract(week from NEW.{self.partition_field}) AS TEXT)',
            'self_check': f"NEW.{self.partition_field} >= '{f_start}' "
            f"AND NEW.{self.partition_field} <= '{f_end}' "
        }

        self.assertTrue(len(results.keys()) == len(expected_results.keys()))
        for k, v in results.items():
            if k != 'partitions':
                print(results[k])
                print(expected_results[k])
                self.assertTrue(results[k] == expected_results[k])

    def test_to_dict_partition_by_week_strict(self):
        self.start = datetime(2019, 12, 1)
        self.end = (self.start + timedelta(days=10)).replace(
            microsecond=999999
        )
        self.time_window = TimePeriod(self.start, self.end)

        self.instance = TemporalDataPartitioner(
            self.parent_table,
            self.partition_field,
            self.time_window,
            partition_by=PartitionByEnum.w,
            index_by=self.index_by,
            template_path=self.template_path,
            template_name=self.template_name,
            strict=True
        )
        results = self.instance.to_dict()
        f_start = self.start.strftime("%Y-%m-%d %H:%M:%S")
        f_end = self.end.strftime("%Y-%m-%d %H:%M:%S.%f")

        expected_results = {
            'name': 'parent_table',
            'partition_prefix': 'parent_table_y2019_w',
            'partitions': [],
            'catch_all_partition_name': 'parent_table_catch_all',
            'partitioned_by': 'week', 'partition_field': 'partition_field',
            'field_value': f'cast(extract(week from NEW.{self.partition_field}) AS TEXT)',
            'self_check': f"NEW.{self.partition_field} >= '{f_start}' "
            f"AND NEW.{self.partition_field} <= '{f_end}' "
        }

        self.assertTrue(len(results.keys()) == len(expected_results.keys()))
        for k, v in results.items():
            if k != 'partitions':
                print(results[k])
                print(expected_results[k])
                self.assertTrue(results[k] == expected_results[k])