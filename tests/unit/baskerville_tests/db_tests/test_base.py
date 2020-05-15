import unittest
from datetime import datetime, timedelta

from baskerville.db.base import (
    GeneratorBase,
    Index,
    TableTools,
    PartitionedTable,
    Partition,
)

from tests.unit.baskerville_tests.helpers.utils import get_default_data_path


class GeneratorSub(GeneratorBase):
    def __init__(self, template_path, template_name):
        super().__init__(template_path, template_name)

    def to_dict(self):
        return self.__dict__


class TestGeneratorBase(unittest.TestCase):
    def setUp(self) -> None:
        self.template_path = get_default_data_path()
        self.template_name = 'sample_template.jinja2'
        self.instance = GeneratorSub(self.template_path, self.template_name)

    def test_instances(self):
        self.assertTrue(hasattr(self.instance, 'template_path'))
        self.assertTrue(hasattr(self.instance, 'template_name'))
        self.assertTrue(hasattr(self.instance, 'j2_env'))
        self.assertTrue(hasattr(self.instance, 'template'))

    def test_rendering(self):
        result = str(self.instance)
        now = datetime.utcnow().strftime("%Y-%m-%d")
        self.assertTrue(now in result)


class TestIndex(unittest.TestCase):
    def setUp(self) -> None:
        self.idx_name = 'text_idx'
        self.idx_table = 'test_table'
        self.idx_fields = ['field1', 'field2', 'field3']
        self.instance = Index(self.idx_name, self.idx_table, self.idx_fields)

    def test_instances(self):
        self.assertTrue(hasattr(self.instance, 'name'))
        self.assertTrue(hasattr(self.instance, 'table_name'))
        self.assertTrue(hasattr(self.instance, 'fields'))

        self.assertTrue(isinstance(self.instance.name, str))
        self.assertTrue(isinstance(self.instance.table_name, str))
        self.assertTrue(isinstance(self.instance.fields, list))

    def test_str(self):
        expected = f'{self.idx_name} ON {self.idx_table} ' \
                   f'({", ".join(self.idx_fields)})'

        self.assertEqual(str(self.instance), expected)

    def test_create(self):
        expected = f'CREATE  INDEX IF NOT EXISTS {self.idx_name} ' \
                   f'ON {self.idx_table} ({", ".join(self.idx_fields)}) ' \
                   f'TABLESPACE pg_default; ' \
                   f'ALTER TABLE {self.idx_table} CLUSTER ON {self.idx_name};'

        self.assertEqual(self.instance.create(), expected)

    def test_drop(self):
        expected = f'DROP INDEX IF EXISTS {self.idx_name};'

        self.assertEqual(self.instance.drop(), expected)


class TestTableTools(unittest.TestCase):
    def test_get_temporal_check_not_new_and(self):
        start = datetime.utcnow()
        end = start + timedelta(seconds=1)
        partition_field = 'some_partition'
        result = TableTools.get_temporal_check(
            partition_field, start, end, new=False, condition='AND'
        )
        expected = f'some_partition >= \'{start.strftime("%Y-%m-%d %H:%M:%S")}\'' \
            f' AND some_partition <= \'{end.strftime("%Y-%m-%d %H:%M:%S.%f")}\' '

        self.assertEqual(result, expected)

    def test_get_temporal_check_not_new_or(self):
        start = datetime.utcnow()
        end = start + timedelta(seconds=1)
        partition_field = 'some_partition'
        result = TableTools.get_temporal_check(
            partition_field, start, end, new=False, condition='OR'
        )
        expected = f'some_partition >= \'{start.strftime("%Y-%m-%d %H:%M:%S")}\'' \
            f' OR some_partition <= \'{end.strftime("%Y-%m-%d %H:%M:%S.%f")}\' '

        self.assertEqual(result, expected)

    def test_get_temporal_check_new_and(self):
        start = datetime.utcnow()
        end = start + timedelta(seconds=1)
        partition_field = 'some_partition'
        result = TableTools.get_temporal_check(
            partition_field, start, end, new=True, condition='AND'
        )
        expected = f'NEW.some_partition >= \'{start.strftime("%Y-%m-%d %H:%M:%S")}\'' \
            f' AND NEW.some_partition <= \'{end.strftime("%Y-%m-%d %H:%M:%S.%f")}\' '

        self.assertEqual(result, expected)


class TestPartitionedTable(unittest.TestCase):
    def setUp(self) -> None:
        self.partition_name = 'partition_name'
        self.partition_field = 'partition_field'
        self.partitioned_by = 'partitioned_by'
        self.index_by = ['index', 'by']
        self.create_catch_all = True

        self.instance = PartitionedTable(
            self.partition_name,
            self.partition_field,
            self.partitioned_by,
            self.index_by,
            create_catch_all=self.create_catch_all
        )

    def test_instances(self):
        self.assertTrue(hasattr(self.instance, 'name'))
        self.assertTrue(hasattr(self.instance, 'partition_field'))
        self.assertTrue(hasattr(self.instance, 'partitioned_by'))
        self.assertTrue(hasattr(self.instance, 'index_by'))
        self.assertTrue(hasattr(self.instance, 'create_catch_all'))
        self.assertTrue(hasattr(self.instance, 'partitions'))

        self.assertTrue(isinstance(self.instance.index_by, list))


class TestPartition(unittest.TestCase):
    def setUp(self) -> None:
        self.partition_name = 'partition_name'
        self.partition_field = 'partition_field'
        self.index_by = list('abcd')
        self.is_catch_all = True
        self.instance = Partition(
            self.partition_name,
            self.partition_field,
            self.index_by,
            is_catch_all=self.is_catch_all
        )

    def test_instances(self):
        self.assertTrue(hasattr(self.instance, 'name'))
        self.assertTrue(hasattr(self.instance, 'partition_field'))
        self.assertTrue(hasattr(self.instance, 'index_by'))
        self.assertTrue(hasattr(self.instance, 'is_catch_all'))

        self.assertEqual(self.instance.name, self.partition_name)
        self.assertEqual(self.instance.partition_field, self.partition_field)
        self.assertEqual(self.instance.index_by, self.index_by)
        self.assertEqual(self.instance.is_catch_all, self.is_catch_all)
