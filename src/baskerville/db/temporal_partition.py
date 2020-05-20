from datetime import datetime, timedelta
from collections import defaultdict
import isoweek
from dateutil.tz import tzutc
from baskerville.util.enums import PartitionByEnum
from baskerville.util.helpers import get_days_in_year, get_days_in_month
from baskerville.db.base import PartitionedTable, Partition, TableTools, Index
from es_retriever.helpers.time_period import TimePeriod as TP


class TimePeriod(TP):
    """
    Extends es_retriever.helpers.time_period.TimePeriod
    """

    def __init__(self, start, end, utc=True):
        super().__init__(start, end)
        self.utc = utc
        if self.utc:
            self.start = self.start.replace(
                tzinfo=tzutc()
            ).replace(microsecond=0)
            self.end = self.end.replace(
                tzinfo=tzutc()
            ).replace(microsecond=999999)

    def __gt__(self, other):
        return self.start > other.start

    def __ge__(self, other):
        return self.start >= other.start

    def __le__(self, other):
        return self.start <= other.start

    def __lt__(self, other):
        return self.start < other.start

    def split_by_year(self) -> list:
        """
        Splits current time period by year
        :return: a list of time periods, each one of them has a start and end
        in a different year,
         e.g. [ TimePeriod(01-01-2018, 31-12-2018),
                TimePeriod(01-01-2019, 31-01-2019)
            ]
        """
        if self.start.year == self.end.year:
            return [self]

        days_in_year = get_days_in_year(self.start.year)
        start = self.start
        end = start + timedelta(days=days_in_year - start.timetuple().tm_yday)
        sub_periods = []

        while end <= self.end:
            sub_periods.append(TimePeriod(start, end))
            days_in_year = get_days_in_year(start.year)
            start = end
            if start.year == self.end.year:
                end = self.end
            else:
                end = start + timedelta(days=days_in_year)

        return sub_periods

    def split_by_year_and_month(self, strict=False) -> dict:
        """
        Splits the current time period by year and by month.
        E.g.:
        {
            2017: {
                12: TimePeriod instance
            },
            2018: {
                1: TimePeriod instance
            }
        }
        :param bool strict: finish at the end of the month (False), or exactly
        at the end date
        :return:
        """
        days_in_end_month = get_days_in_month(self.end.year, self.end.month)
        start_m = self.start
        end_m = self.end
        if not strict:
            start_m = self.start.replace(day=1, hour=0, minute=0, second=0)
            end_m = self.end.replace(
                                    day=days_in_end_month,
                                    hour=23,
                                    minute=59,
                                    second=59
                                )

        if self.start.year == self.end.year:
            if self.start.month == self.end.month:
                if strict:
                    return {
                        self.start.year: {
                            self.start.month: TimePeriod(
                                start_m,
                                end_m
                            )
                        }
                    }
                return {
                    self.start.year: {self.start.month: self}
                }

        sub_periods = {}

        for year_tw in self.split_by_year():
            y = year_tw.start.year
            m = year_tw.start.month
            days_in_month = get_days_in_month(self.start.year,
                                              self.start.month)
            start = year_tw.start
            end = start.replace(
                day=days_in_month,
                hour=23,
                minute=59,
                second=59,
                microsecond=59
            )
            curr_period = {y: defaultdict(list)}
            for i in range(m, 13):
                curr_period[y][i].append(TimePeriod(start, end))
                start = datetime(y, i, 1, 0, 0, 0, 0).replace(tzinfo=tzutc())
                days_in_month = get_days_in_month(start.year, i)
                if i == self.end.month and strict:
                    end = self.end
                else:
                    end = start.replace(
                        day=days_in_month,
                        hour=23,
                        minute=59,
                        second=59,
                        microsecond=59
                    )
            sub_periods.update(curr_period)

        print(sub_periods)
        return sub_periods

    def split_by_year_and_week(self, strict=False) -> dict:
        """
        Splits the current time period by year and by week.
        E.g.:
        {
            2017: {
                52: TimePeriod instance
            },
            2018: {
                0: TimePeriod instance
            }
        }
        :param bool strict: finish at the end of the week (False), or exactly
        at the end date
        :return:
        """
        sub_periods = {}
        start_y, start_w, _ = self.start.isocalendar()
        end_y, end_w, _ = self.end.isocalendar()

        if start_y == end_y and start_w == end_y:
            if strict:
                return {
                    start_y: {start_w: self}
                }
            else:
                week = isoweek.Week(start_y, start_w)
                start_wd = datetime(*week.monday().timetuple()[:6]).replace(
                    hour=0, minute=0, second=0
                )
                end_wd = datetime(*week.sunday().timetuple()[:6]).replace(
                    hour=23, minute=59, second=59
                )
                return {
                    start_y: {start_w: TimePeriod(
                                start_wd,
                                end_wd
                            )}
                }

        for year_tw in self.split_by_year():
            start_range = 0
            y = year_tw.start.year
            curr_period = {y: defaultdict(list)}
            weeks_in_year = isoweek.Week.last_week_of_year(y).week

            if y == self.end.year and y == end_y:
                weeks_in_year = self.end.isocalendar()[1]

            if y == self.start.year and y == start_y:
                start_range = self.start.isocalendar()[1]

            for w in range(start_range, weeks_in_year + 1):
                week = isoweek.Week(y, w)
                if y == self.start.year and w == start_range and strict:
                    start = self.start
                else:
                    start = datetime(
                        *week.monday().timetuple()[:6]
                    ).replace(hour=0, minute=0, second=0)
                    if y == self.start.year and y == start_y and w == start_range:
                        start = start.replace(
                            hour=self.start.hour,
                            minute=self.start.minute,
                            second=self.start.second
                        )
                if y == self.end.year and w == end_w and strict:
                    end = self.end
                else:
                    end = datetime(
                        *week.sunday().timetuple()[:6]
                    ).replace(hour=23, minute=59, second=59)
                curr_period[y][w] = TimePeriod(start, end)
            sub_periods.update(curr_period)

            if end_y not in sub_periods:
                week = isoweek.Week(end_y, end_w)
                start = datetime(
                    *week.monday().timetuple()[:6]).replace(
                                hour=0, minute=0, second=0
                )
                end = self.end
                if not strict:
                    end = datetime(*week.sunday().timetuple()[:6]).replace(
                                hour=23, minute=59, second=59)
                sub_periods[end_y] = {end_w: TimePeriod( start, end)}

        return sub_periods


class TemporalPartitionedTable(PartitionedTable):
    """
    Representation of a partitioned by date table.
    """
    def __init__(
            self, name, active_period, partition_field,
            partitioned_by=PartitionByEnum.w, index_by=None,
            create_catch_all=True, strict=False
    ):
        super().__init__(
            name, partition_field, partitioned_by, index_by, create_catch_all
        )
        self.strict = strict
        self.active_period = active_period
        self._partition_prefix = self.get_partition_prefix()
        self.start_year = self.active_period.start.year

    @property
    def partition_prefix(self):
        """
        The prefix of the partitioned table e.g.
        :return:
        """
        return self._partition_prefix.replace(
            '%year', str(self.active_period.start.year)
        ).replace('%unit', '')

    @property
    def start(self):
        if self.partitions:
            return min(self.partitions).active_period.start
        return self.active_period.start

    @property
    def end(self):
        if self.partitions:
            p = max(self.partitions)
            if p.is_catch_all:
                p = self.partitions[-2]
            return p.active_period.end
        return self.active_period.end

    def self_check(self):
        return TableTools.get_temporal_check(
            self.partition_field,
            self.start,
            self.end,
            new=True
        )

    @property
    def field_value(self) -> str:
        return f'cast(extract({self.partitioned_by} ' \
            f'from NEW.{self.partition_field}) AS TEXT)'

    def get_partition_prefix(self) -> str:
        """
        The table prefix, in the form of:
        table_to_be_partitioned_%unit
        where unit will be replaced by the partition type
        (year, month, week etc)
        :return:
        """
        return f'{self.name}_y%year_{str(self.partitioned_by)[0]}%unit'

    def get_partition_name(self, year, unit) -> str:
        return self._partition_prefix.replace(
            '%year', str(year)
        ).replace(
            '%unit', str(unit)
        )

    def get_partition_range(self) -> dict:
        """
        Return a dict with the date range for the partitions
        e.g.
        for the weekly partition:
         {
            2017: {
                52: TimePeriod instance
            },
            2018: {
                0: TimePeriod instance
            }
        }
        :return:
        :rtype: dict[int][TimePeriod]
        """
        if self.partitioned_by == PartitionByEnum.w:
            return self.active_period.split_by_year_and_week(self.strict)
        elif self.partitioned_by == PartitionByEnum.m:
            return self.active_period.split_by_year_and_month(self.strict)
        else:
            raise ValueError(
                f'Unknown split by option {str(self.partitioned_by)}')

    def partition(self) -> list:
        """
        Partition the table by the partition range
        :return: A list of partitions to be created
        :rtype: list[TemporalPartition]
        """
        for y, units in self.get_partition_range().items():
            for i, tw in units.items():
                self.partitions.append(self.get_partition_for(i, tw, y))
        if self.create_catch_all:
            self.partitions.append(self.get_catch_all_partition())

        return sorted(self.partitions, key=lambda p: p.active_period)

    def get_partition_for(self, unit, tw, year):
        """
        Returns a TemporalPartition for the specific time period (tw) and year
        - given a unit: month | week
        :param str unit: m (for month) or w (for week)
        :param TimePeriod tw:
        :param int year:
        :return:
        :rtype: TemporalPartition
        """
        return TemporalPartition(
            self.get_partition_name(year, unit),
            tw,
            self.partition_field,
            self.index_by
        )

    def get_catch_all_partition(self):
        """
        Create a TemporalPartition that will serve as a catch all partition
        :return:
        :rtype: TemporalPartition
        """
        start = self.end + timedelta(seconds=1)
        end = self.start - timedelta(seconds=1)
        return TemporalPartition(
            self.catch_all_partition_name,
            TimePeriod(start, end),
            self.partition_field,
            self.index_by,
            is_catch_all=True
        )

    def to_dict(self):
        return {
            'name': self.name,
            'partition_prefix': self.partition_prefix,
            'partitions': [p.to_dict() for p in self.partitions],
            'catch_all_partition_name': self.catch_all_partition_name,
            'partitioned_by': str(self.partitioned_by),
            'partition_field': self.partition_field,
            'field_value': self.field_value,
            'self_check': self.self_check()
        }


class TemporalPartition(Partition):
    def __init__(self, name, active_period, partition_field, index_by=None,
                 is_catch_all=False):
        super().__init__(name, partition_field, index_by, is_catch_all)
        self.name = name
        self.active_period = active_period
        self.partition_field = partition_field
        self.index_by = index_by
        self.indexes = []

    def __lt__(self, other):
        if isinstance(other, TemporalPartition):
            return self.active_period < other.active_period
        if isinstance(other, datetime):
            return self.active_period.start < other

    def __le__(self, other):
        if isinstance(other, TemporalPartition):
            return self.active_period <= other.active_period
        if isinstance(other, datetime):
            return self.active_period.start <= other

    def __gt__(self, other):
        if isinstance(other, TemporalPartition):
            return self.active_period > other.active_period
        if isinstance(other, datetime):
            return self.active_period.end > other

    def __ge__(self, other):
        if isinstance(other, TemporalPartition):
            return self.active_period >= other.active_period
        if isinstance(other, datetime):
            return self.active_period.start >= other

    @property
    def constraint_check(self):
        return TableTools.get_temporal_check(
            self.partition_field,
            self.active_period.start,
            self.active_period.end,
            new=False
        )

    @property
    def self_check(self):
        return TableTools.get_temporal_check(
            self.partition_field,
            self.active_period.start,
            self.active_period.end,
            new=False
        )

    @property
    def index(self):
        if self.index_by:
            return Index(
                f'idx_{self.name}_{"_".join(self.index_by)}',
                self.name,
                self.index_by
            )

    def to_dict(self):
        return {
            'name': self.name,
            'start': self.active_period.start,
            'end': self.active_period.end,
            'constraint_check': self.constraint_check,
            'self_check': self.self_check,
            'is_catch_all': self.is_catch_all,
            'indexes': [self.index.to_dict()] if self.index else [],
            # todo: single index support for now
        }
