# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import dateutil.parser
from datetime import datetime

from baskerville.db.base import GeneratorBase, TEMPLATE_FOLDER
from baskerville.db.temporal_partition import TemporalPartitionedTable, \
    TimePeriod
from baskerville.util.enums import PartitionByEnum
from jinja2 import Environment, FileSystemLoader


class DataPartitioner(GeneratorBase):
    """
    Base partitioner, generates the data that the partitioning template needs
    """

    def __init__(
            self,
            parent_table,
            partition_field,
            index_by=None,
            template_path=TEMPLATE_FOLDER,
            template_name='data_partitioning.jinja2'
    ):
        super().__init__(
            template_path=template_path, template_name=template_name
        )
        self.parent_table = parent_table
        self.partition_field = partition_field
        self.index_by = index_by
        self.partitions = []

    def to_dict(self):
        return self.__dict__


class TemporalDataPartitioner(DataPartitioner):
    """
    Partition data based on a date field. Generates
    """

    def __init__(
            self,
            partitioned_table_name,
            partition_field,
            time_window,
            strict=False,
            partition_by=PartitionByEnum.w,
            index_by=None,
            template_path=TEMPLATE_FOLDER,
            template_name='data_partitioning.jinja2'
    ):
        """
        Partitions the parent table by a time period, e.g. w for per week
        partitions, m for per month partitions.

        :param str partitioned_table_name: the name of the parent table
        :param str partition_field: the name of the field to partition by
        :param baskerville.db.base.TimePeriod time_window: the period to
        partition data for, e.g. a year, starting from 1-1-2018 to 31-12-2018
        :param str split_by: w for week, m for month
        :param index_by: list of fields to index the partitions by
        """
        super().__init__(
            partitioned_table_name,
            partition_field,
            index_by=index_by,
            template_path=template_path,
            template_name=template_name,
        )
        self.strict = strict
        self.partitioned_table = TemporalPartitionedTable(
            partitioned_table_name,
            time_window,
            partition_field,
            partitioned_by=partition_by,
            index_by=index_by,
            strict=self.strict
        )
        self.partitions = self.partitioned_table.partition()

    def to_dict(self):
        return self.partitioned_table.to_dict()


class AlphabeticalDataPartitioner(DataPartitioner):
    partition_by = list('abcdefghijklmnopqrtsuvwxyz')


class RangeDataPartitioner(DataPartitioner):
    pass


def get_temporal_partitions(conf):
    """
    Given the database configuration, construct the sql to be executed to
    partition a table (set in the configuration) by dates, using a
    TemporalDataPartitioner
    :param dict[str, T] conf: the dict representation of the database
    configuration
    :return: The string representation of the sql to be executed to partition
    a table by a time period.
    """
    from datetime import date

    since = conf['data_partition']['since']
    until = conf['data_partition']['until']

    if isinstance(since, date):
        since = datetime(*since.timetuple()[:6])

    if isinstance(until, date):
        until = datetime(*until.timetuple()[:6])

    return str(TemporalDataPartitioner(
        conf['partition_table'],
        conf['partition_field'],
        time_window=TimePeriod(
            since,
            until
        ),
        partition_by=conf['partition_by'],
        index_by=conf['data_partition']['index_by'],
        template_path=conf['template_folder'] or TEMPLATE_FOLDER,
        template_name=conf['data_partition']['template']
    )
    )


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'parentname', help='The name of the table to be partitioned'
    )
    parser.add_argument('field', help='The table field to partition by')
    parser.add_argument(
        'ptype', help='The type of data partitioner, e.g. temporal'
    )
    parser.add_argument(
        '-s', '--since', help='Optional', type=dateutil.parser.parse
    )
    parser.add_argument(
        '-u', '--until', help='The type of data partitioner',
        type=dateutil.parser.parse
    )
    parser.add_argument(
        '-b', '--splitby', help='Split the data by week or month'
    )
    parser.add_argument('-st', '--strict', help='tbd', default=False)
    parser.add_argument(
        '-i', '--indexby', help='Index the data', dest='indexby',
        type=lambda s: [str(item) for item in s.split(',')]
    )
    args = parser.parse_args()

    if args.ptype == 'temporal':
        if not args.since or not args.until or not args.splitby:
            raise ValueError(
                'Temporal partitioner needs since and until and splitby'
            )

    tp = TemporalDataPartitioner(
        args.parentname,
        args.field,
        time_window=TimePeriod(args.since, args.until),
        partition_by=args.splitby,
        index_by=args.indexby
    )

    print(tp.partitions)
    # Create the jinja2 environment - trim_blocks helps control whitespace.
    j2_env = Environment(loader=FileSystemLoader(TEMPLATE_FOLDER),
                         trim_blocks=True)
    # get template and render
    template = j2_env.get_template("data_partitioning.jinja2")
    template.globals['now'] = datetime.utcnow
    rendered_data = template.render(tp.to_dict())

    with open('test.sql', 'wb') as out_file:
        out_file.write(bytes(rendered_data.encode('utf-8')))


# https://stackoverflow.com/a/23206636/3433323
