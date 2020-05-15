from datetime import datetime

import dateutil
from baskerville.db.base import GeneratorBase, TEMPLATE_FOLDER
from baskerville.db.temporal_partition import TemporalPartitionedTable, \
    TimePeriod
from baskerville.util.enums import PartitionByEnum
from baskerville.util.helpers import get_default_data_path, get_days_in_year


class Archiver(GeneratorBase):
    """
    Generates the data to be used in the archive template to generate the
    necessary sql for data archive
    """
    def __init__(
            self,
            partitioned_table_name,
            partition_field,
            archive_table_name=None,
            template_path=TEMPLATE_FOLDER,
            template_name='data_archiving.jinja2'
    ):
        super().__init__(template_path, template_name)
        self.partitioned_table_name = partitioned_table_name
        self.partition_field = partition_field
        self.archive_table_name = archive_table_name

    def get_name(self):
        return f'{self.partitioned_table_name}_archive'

    def to_dict(self):
        return {
            'name': self.name,
            'parent_table': self.parent_table,
            'partitions_to_detach': self.partitions_to_detach,
        }

    def __str__(self):
        self.template.globals['now'] = datetime.utcnow
        return super().__str__()


class TemporalArchiver(Archiver):
    """
    The temporal archiver must check for partitions to archive within the
    archive period (TimePeriod), detach them from the partitioned table and
    either attach them to the archive table as partitions or delete them.
    The on before insert trigger of the partitioned table must be modified to
    include the non archived period - but this is not part of the archiver,
    please, run the data partition process separately.
    """
    def __init__(
            self,
            partitioned_table_name,
            partition_field,
            archive_period,
            archive_table_name=None,
            partition_by=PartitionByEnum.w,
            index_by=None,
            template_path=TEMPLATE_FOLDER,
            template_name='data_archiving.jinja2',
    ):
        super().__init__(
            partitioned_table_name,
            partition_field,
            archive_table_name,
            template_path,
            template_name
        )

        self.archive_period = archive_period
        self.archive_table_name = archive_table_name or self.get_name()
        self.index_by = index_by
        self.retention_period = get_days_in_year(self.archive_period.start.year)

        self.partitioned_table = TemporalPartitionedTable(
            self.partitioned_table_name,
            self.archive_period,
            partition_field,
            partitioned_by=partition_by,
            index_by=index_by
        )
        self.partitioned_table.catch_all_partition_name = \
            f'{self.archive_table_name}_catch_all'
        self.partitioned_table.partition()

    def get_name(self):
        """
        Returns the archive table name, e.g. tablename_archive_2017_to_2018
        :return:
        """
        if self.archive_period.start.year == self.archive_period.end.year:
            return f'{self.partitioned_table_name}_archive_' \
                f'y{self.archive_period.start.year}'
        return f'{self.partitioned_table_name}_archive_' \
                f'y{self.archive_period.start.year}_to_' \
            f'y{self.archive_period.end.year}'

    def to_dict(self):
        d = self.partitioned_table.to_dict()
        d['parent_table'] = self.partitioned_table_name
        d['name'] = self.archive_table_name
        return d


def get_archive_script(since, until, parent_table='request_sets'):
    """
    Creates an Archiver based on the configuration and returns the parsed sql
    for the archive table creation and the partition detachment/ re-attachment
    to the archive table
    :param datetime.datetime since: since when to archive
    :param datetime.datetime until: until when to archive
    :param str parent_table:
    :return: the string representation of the sql archive script to be executed
    """
    archiver = TemporalArchiver(
        parent_table,
        'created_at',
        TimePeriod(
                since,
                until,
            ),
        )

    return str(archiver)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'parentname', help='The name of the table to be partitioned'
    )
    parser.add_argument('field', help='The table field to partition by')
    parser.add_argument(
        'splitby', help='Split the data by either week or month'
    )
    parser.add_argument(
        '-s', '--since',
        help='Start date of archive period',
        type=dateutil.parser.parse
    )
    parser.add_argument(
        '-u', '--until',
        help='End date of archive period',
        type=dateutil.parser.parse
    )

    parser.add_argument(
        '-i', '--indexby', help='Index the data', dest='indexby',
        type=lambda s: [str(item) for item in s.split(',')]
    )
    args = parser.parse_args()

    if not args.since or not args.until:
        raise ValueError(
            'Archiver needs since and until'
        )
    if args.splitby not in ['week', 'month']:
        raise NotImplementedError()

    tp = TemporalArchiver(
        args.parentname,
        args.field,
        archive_period=TimePeriod(args.since, args.until),
        partition_by=args.splitby,
        index_by=args.indexby
    )

    with open('archive_test.sql', 'w') as f:
        f.write(str(tp))
