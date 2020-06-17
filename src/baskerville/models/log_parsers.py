# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import abc
import warnings

import warlock
import pyspark.sql.types as T
import pyspark.sql.functions as F
from baskerville.util.helpers import SerializableMixin


class LogParser(object, metaclass=abc.ABCMeta):

    def __init__(self, schema, drop_row_if_missing):
        self.__schema = schema
        self.drop_row_if_missing = drop_row_if_missing

    @property
    def schema(self):
        return self.__schema

    @schema.setter
    def schema(self, value):
        self.__schema = value

    @property
    @abc.abstractmethod
    def schema_class(self):
        pass

    @abc.abstractmethod
    def fill_missing_values(self, df):
        pass

    @abc.abstractmethod
    def check_for_missing_columns(self, df):
        pass

    @abc.abstractmethod
    def add_missing_columns(self, df, missing):
        pass

    @abc.abstractmethod
    def drop_if_missing_filter(self):
        """
        Filter out not null
        :return:
        """
        pass


class JSONLogParser(LogParser, SerializableMixin):

    def __init__(
            self,
            schema,
            drop_row_if_missing=None
    ):
        super(JSONLogParser, self).__init__(schema, drop_row_if_missing)
        self.__schema_class = warlock.model_factory(self.schema)
        self.required = self.schema['required']

    @property
    def schema_class(self):
        return self.__schema_class

    def fill_missing_values(self, df, required_only=False):
        """
        Checks the columns in a dataframe, if any column is missing, add it and
        fill it with the default value. If no default value specified, nulls
        will remain the same.
        :param pandas.DataFrame df: the dataframe to fill missing values for
        :param boolean required_only: if True, only the values of the required
        columns will be filled in. Defaults to False, all missing values are
        filled in.
        :return: df with specified default values filled in
        :rtype: pandas.DataFrame
        """
        for c in df.columns:
            if c in self.schema['properties']:
                default = self.schema['properties'][c].get('default')
                if default:
                    df[c].fillna(default, inplace=True)
                else:
                    warnings.warn(f'No default value specified for {c}.')

        return df

    def check_for_missing_columns(self, df, required_only=True):
        """

        :param df:
        :param boolean required_only: look only for missing required columns
        :return:
        """
        cols = self.schema['properties'].keys()
        if required_only:
            cols = self.required

        if not cols or set(cols).issubset(set(df.columns)):
            return []

        return list(set(cols) - set(df.columns))

    def add_missing_columns(self, df, missing):
        """

        :param df:
        :param missing:
        :return:
        """
        for c in missing:
            default = self.schema['properties'][c].get('default')
            df[c] = df.apply(lambda row: default)

        return df

    def drop_if_missing_filter(self):
        pass


class JSONLogSparkParser(JSONLogParser, SerializableMixin):

    def __init__(self, schema, drop_row_if_missing=None, sample=None):

        self.str_to_type = {
            'string': (lambda sample: T.StringType()),
            'number': (lambda sample: T.FloatType()),
            'integer': (lambda sample: T.IntegerType()),
        }
        self.sample = sample

        super().__init__(schema, drop_row_if_missing)

        # back up json schema
        self.__json_schema = schema

        def add_json_item(parent, name, value):
            if 'type' in value.keys():
                parent.add(T.StructField(
                    name,
                    self.str_to_type[value['type']](value.get('default')),
                    not value.get('required', False)
                ))
                return

            struct_value = T.StructType()
            for child_name, child_value in value.items():
                add_json_item(struct_value, child_name, child_value)

            parent.add(T.StructField(name, struct_value))

        self.__schema = T.StructType()
        for child_name, child_value in schema['properties'].items():
            add_json_item(self.__schema, child_name, child_value)

        self.schema = self.__schema
        self.__schema_class = self.__json_schema

    @property
    def schema_class(self):
        return self.__schema_class

    def fill_missing_values(self, df, required_only=False):
        """

        :param pyspark.sql.DataFrame df: the dataframe to fill missing values
        for
        :param boolean required_only: if True, only the values of the required
        columns will be filled in. Defaults to False, all missing values are
        filled in.
        :return:
        """
        properties = self.__json_schema['properties']

        def fill(c):
            return (required_only and c in self.required) or not required_only

        for c in df.columns:
            if c in properties:
                if fill(c):
                    default = properties[c].get('default', None)
                    if default is not None:
                        df = df.fillna(default, subset=[c])
                    else:
                        warnings.warn(f'Default value for {c} is None.')
                else:
                    # required flag is set and c not in required
                    warnings.warn(
                        f'Skip field {c} null value filling.'
                    )
        return df

    def add_missing_columns(self, df, missing):
        from pyspark.sql import functions as F
        for c in missing:
            if c not in df.columns:
                default = self.__json_schema['properties'][c].get(
                    'default', None
                )
                df = df.withColumn(c, F.lit(default))
            else:
                warnings.warn(
                    f'{c} was declared as a missing column, '
                    f'but it exists in df.'
                )

        return df

    def check_for_missing_columns(self, df, required_only=True):
        """
        Checks if any columns are missing from the dataframe.
        Returns a list of the missing columns
        :param pyspark.sql.DataFrame df:
        :param boolean required_only: look only for missing required columns
        :return: the list of missing column names
        :rtype: list[str]
        """
        cols = self.__json_schema['properties'].keys()
        if required_only:
            cols = self.required

        if not cols or set(cols).issubset(set(df.columns)):
            return []

        return list(set(cols) - set(df.columns))

    def drop_if_missing_filter(self):
        """
        Filter out not null
        :return:
        """
        if self.drop_row_if_missing:
            filter_ = F.col(self.drop_row_if_missing[0]).isNotNull()

            for c in self.drop_row_if_missing:
                filter_ = filter_ & F.col(c).isNotNull()
        else:
            filter_ = None

        return filter_


LOG_PARSERS = {
    JSONLogParser.__name__: JSONLogParser,
    JSONLogSparkParser.__name__: JSONLogSparkParser
}
