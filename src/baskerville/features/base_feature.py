import abc
import stringcase
from collections import OrderedDict

from baskerville.util.helpers import SerializableMixin
from pyspark.sql import functions as F

from baskerville.util.enums import FeatureComputeType


class BaseFeature(SerializableMixin, metaclass=abc.ABCMeta):
    """
    The parent class for all features used to distinguish an attack from a
    legitimate request. IPSieve provides the feature with a dictionary and the
    feature returns a dictionary of IPs and the feature's numerical value.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = []
    CACHE_COLUMNS = []
    DEPENDENCIES = []
    _prefix = 'Feature'

    def __init__(self):
        self.group_by_aggs = {}
        self.pre_group_by_calcs = {}
        self.post_group_by_calcs = {}
        self.columns_renamed = {}
        self.cache_columns_defaults = {}

    def __str__(self):
        return self.feature_name_from_class()

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.feature_name} ' \
               f'default value: {self.DEFAULT_VALUE}, ' \
               f'dependencies:{",".join(self.DEPENDENCIES)}, ' \
               f'columns:{",".join(self.COLUMNS)}>'

    @property
    def columns(self):
        return self.COLUMNS

    @property
    def cache_columns(self):
        return self.CACHE_COLUMNS

    @property
    def dependencies(self):
        return self.DEPENDENCIES

    @property
    def feature_name(self):
        return stringcase.snakecase(self.__class__.__name__.replace(self._prefix, ''))

    @classmethod
    def feature_name_from_class(cls):
        return stringcase.snakecase(cls.__name__.replace(cls._prefix, ''))

    @property
    def feature_default(self):
        return self.DEFAULT_VALUE

    def compute(self, *args, **kwargs):
        """
        The feature should overload this function to implement the feature
        computation. At the end the results should be stored
        in a dictionary with the format of IP:value where the value is a double
        or an integer value
        """
        raise NotImplementedError('Base class\' compute was called. '
                                  'Please, provide implementation')

    def misc_compute(self, logs_df):
        return logs_df

    @classmethod
    def update_row(cls, current, past, **kwargs):
        """
        Given the past feature value, update the current feature value according
        to the feature calculation type: average by default
        :param dict[str, float] current:
        :param dict[str, float] past:
        :param dict[T] kwargs:
        :return:
        """
        raise NotImplementedError('Base class\' update was called. '
                                  'Please, provide implementation')


class TimeBasedFeature(BaseFeature, metaclass=abc.ABCMeta):
    CACHE_COLUMNS = ['first_ever_request']
    date_format = "YYYY-MM-DD %H:%M:%S"

    def __init__(self):
        super().__init__()
        self.group_by_aggs = {
            'first_request': F.min(F.col('@timestamp')),
            'last_request': F.max(F.col('@timestamp')),
        }

        # need to preserve the order here
        self.post_group_by_calcs = OrderedDict({
            'start': (
                F.when(
                    F.col('first_ever_request').isNotNull(),
                    F.col('first_ever_request')
                ).otherwise(F.col('first_request'))
            ).cast('timestamp'),
            'dt': (
                (
                    F.unix_timestamp(
                        F.col('last_request'), format=self.date_format
                    ) -
                    F.unix_timestamp(
                        F.col('start'), format=self.date_format
                    )
                ).cast('float') / 60.
            )
        })

        self.cache_columns_defaults = {
            # todo: will this add a 1970?
            'first_ever_request': F.lit(None).cast('timestamp')
        }

    def compute(self, df):
        return df


class UpdateableFeature(BaseFeature, metaclass=abc.ABCMeta):
    _updated_prefix = '_updated_'
    _update_type = FeatureComputeType.other
    current_features_column = 'features'
    past_features_column = 'old_features'

    @property
    def updated_feature_col_name(self):
        return f'{self._updated_prefix}{self.feature_name}'

    @property
    def compute_type(self):
        return self._update_type

    @abc.abstractmethod
    def update(
            self,
            df,
            feat_column='features',
            old_feat_column='old_features',
            *args,
            **kwargs
    ):
        pass
