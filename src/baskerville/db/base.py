import abc
from datetime import datetime

from baskerville.util.helpers import get_default_data_path
from jinja2 import Environment, FileSystemLoader


TEMPLATE_FOLDER = f'{get_default_data_path()}/templates'


class GeneratorBase(object, metaclass=abc.ABCMeta):
    def __init__(
            self,
            template_path=TEMPLATE_FOLDER,
            template_name='data_partitioning.jinja2'
    ):
        self.template_path = template_path
        self.template_name = template_name
        self.j2_env = Environment(
            loader=FileSystemLoader(self.template_path),
            trim_blocks=True
        )
        self.template = self.j2_env.get_template(self.template_name)

    def __str__(self):
        self.template.globals['now'] = datetime.utcnow
        return self.template.render(self.to_dict())

    @abc.abstractmethod
    def to_dict(self):
        pass


class Index(object):
    def __init__(self, name, table_name, fields, unique=False):
        self.name = name
        self.table_name = table_name
        self.fields = fields
        self.unique = unique

    def __str__(self):
        return f'{self.name} ON {self.table_name} ({", ".join(self.fields)})'

    def create(self):
        # todo - rethink about clustering
        return f'CREATE {"UNIQUE" if self.unique else ""} INDEX ' \
               f'IF NOT EXISTS {str(self)} ' \
               f'TABLESPACE pg_default; ' \
               f'ALTER TABLE {self.table_name} CLUSTER ON {self.name};'

    def drop(self):
        return f'DROP INDEX IF EXISTS {self.name};'

    def to_dict(self):
        return {
            'create': self.create(),
            'drop': self.drop(),
        }


class TableTools(object):

    @staticmethod
    def get_temporal_check(partition_field, start, end, new=False,
                           condition='AND'):
        new_prefix = ''
        if new:
            new_prefix = 'NEW.'

        start = start.strftime("%Y-%m-%d %H:%M:%S")
        end = end.strftime("%Y-%m-%d %H:%M:%S.%f")

        if start > end:
            condition = 'OR'

        return f'{new_prefix}{partition_field} >= \'{start}\' ' \
            f'{condition} {new_prefix}{partition_field} <= \'{end}\' '


class PartitionedTable(object):
    def __init__(
            self,
            name,
            partition_field,
            partitioned_by,
            index_by,
            create_catch_all=True
    ):
        self.name = name
        self.partition_field = partition_field
        self.partitioned_by = partitioned_by
        self.index_by = index_by
        self.create_catch_all = create_catch_all
        self.catch_all_partition_name = f'{self.name}_catch_all'
        self.partitions = []


class Partition(object):
    def __init__(
            self,
            name,
            partition_field,
            index_by=None,
            is_catch_all=False
    ):
        self.name = name
        self.partition_field = partition_field
        self.index_by = index_by
        self.is_catch_all = is_catch_all
