import os
import json
import unittest
import pandas as pd

from pyspark.sql import functions as F, types as T

import baskerville
from baskerville.models.log_parsers import JSONLogParser, JSONLogSparkParser

from tests.unit.baskerville_tests.helpers.spark_testing_base import \
    SQLTestCaseLatestSpark


class TestJSONLogParser(unittest.TestCase):
    def setUp(self):
        sample_schema_path = f'{os.path.dirname(baskerville.__file__)}' \
                             f'/../../data/samples/sample_log_schema.json'
        with open(sample_schema_path) as schema_file:
            self.schema = json.load(schema_file)

    def test_instance(self):
        jlp = JSONLogParser(self.schema)
        data = {
            'timestamp': '2018-04-17T08:12:32.000Z',
            'some_property': 'somevalue',
        }
        instance = jlp.schema_class(**data)
        for k, v in data.items():
            self.assertTrue(hasattr(instance, k))
            self.assertTrue(getattr(instance, k) == v)

        self.assertTrue(instance.schema == self.schema)

    def test_fill_missing_values_no_defaults(self):
        jlp = JSONLogParser(self.schema)

        data = {
            'timestamp': [
                '2018-04-17T08:12:32.000Z',
                '2018-04-17T09:12:32.000Z',
                None
            ],
            'some_property': ['somevalue', None, 'othervalue'],
        }
        df = pd.DataFrame(data)

        result_df = jlp.fill_missing_values(df)

        self.assertTrue(
            result_df.values.tolist() == df.values.tolist()
        )

    def test_fill_missing_values(self):
        self.schema['properties']['timestamp']['default'] = \
            '2018-01-01T00:00:00.000Z'
        self.schema['properties']['some_property']['default'] = 'DEFAULT'

        jlp = JSONLogParser(self.schema)

        data = {
            'timestamp': [
                '2018-04-17T08:12:32.000Z',
                '2018-04-17T09:12:32.000Z',
                None
            ],
            'some_property': ['somevalue', None, 'othervalue'],
        }
        df = pd.DataFrame(data)

        result_df = jlp.fill_missing_values(df)
        df['timestamp'].fillna('2018-01-01T00:00:00.000Z', inplace=True)
        df['some_property'].fillna('DEFAULT', inplace=True)

        self.assertTrue(
            result_df.values.tolist() == df.values.tolist()
        )

    def test_check_for_missing_columns(self):
        jlp = JSONLogParser(self.schema)

        data = {
            'timestamp': [
                '2018-04-17T08:12:32.000Z',
                '2018-04-17T09:12:32.000Z',
                None
            ],
            'some_property': ['somevalue', None, 'othervalue'],
        }
        df = pd.DataFrame(data)

        missing = jlp.check_for_missing_columns(df)

        self.assertTrue(
            missing == []
        )
        mdf = df.drop(columns=['some_property'])
        missing = jlp.check_for_missing_columns(mdf)

        self.assertTrue(
            missing == ['some_property']
        )

    def test_add_missing_columns(self):
        jlp = JSONLogParser(self.schema)

        data = {
            'timestamp': [
                '2018-04-17T08:12:32.000Z',
                '2018-04-17T09:12:32.000Z',
                None
            ],
        }
        df = pd.DataFrame(data)

        missing = jlp.check_for_missing_columns(df)

        self.assertTrue(
            missing == ['some_property']
        )
        mdf = jlp.add_missing_columns(df, missing)
        self.assertTrue(list(mdf.columns) == jlp.required)


class TestJSONLogSparkParser(SQLTestCaseLatestSpark):
    def setUp(self):
        super().setUp()
        sample_schema_path = f'{os.path.dirname(baskerville.__file__)}' \
                             f'/../../data/samples/sample_log_schema.json'
        with open(sample_schema_path) as schema_file:
            self.schema = json.load(schema_file)

    def test_instance(self):
        required = ['timestamp', 'some_property']
        jlp = JSONLogSparkParser(self.schema)
        data = {
            'timestamp': '2018-04-17T08:12:32.000Z',
            'some_property': 'somevalue',
        }
        schema = T.StructType(
            [

                    T.StructField("timestamp", T.StringType(), True),
                    T.StructField("some_property", T.StringType(),True),
                    T.StructField("some_other_property", T.StringType(), True),
                    T.StructField("an_integer_property", T.IntegerType(), True),
                    T.StructField("a_numeric_property", T.StringType(), True)

            ]
        )
        self.assertListEqual(required, jlp.required)
        self.assertTrue(jlp.schema_class == self.schema)
        self.assertTrue(jlp.schema == schema)

    def test_fill_missing_values(self):
        self.schema['properties']['timestamp']['default'] = \
            '2018-01-01T00:00:00.000Z'
        self.schema['properties']['some_property']['default'] = 'DEFAULT'

        jlp = JSONLogSparkParser(self.schema)

        data = [{
            'timestamp': '2018-04-17T08:12:32.000Z',
            'some_property': 'somevalue',
        },
            {
                'timestamp': '2018-04-17T08:12:32.000Z',
                'some_property': None,
            },
            {
                'timestamp': None,
                'some_property': 'somevalue',
            }
        ]

        df = self.session.createDataFrame(data)

        result_df = jlp.fill_missing_values(df)
        mdf = df.fillna('2018-01-01T00:00:00.000Z', subset=['timestamp'])
        mdf = mdf.fillna('DEFAULT', subset=['some_property'])

        # fix nullables
        mdf = self.fix_schema(mdf, result_df.schema, mdf.columns)

        self.assertDataFrameEqual(result_df, mdf)

        extra_df = df.withColumn('some_other_property', F.lit(None))
        result_df = jlp.fill_missing_values(extra_df, required_only=True)
        mdf = extra_df.fillna('2018-01-01T00:00:00.000Z', subset=['timestamp'])
        mdf = mdf.fillna('DEFAULT', subset=['some_property'])

        extra_df.show()
        mdf.show()

        # fix nullables
        mdf = self.fix_schema(mdf, result_df.schema, mdf.columns)

        self.assertDataFrameEqual(result_df, mdf)

    def test_check_for_missing_columns(self):
        self.schema['properties']['timestamp']['default'] = \
            '2018-01-01T00:00:00.000Z'
        self.schema['properties']['some_property']['default'] = 'DEFAULT'

        jlp = JSONLogSparkParser(self.schema)

        data = [{
            'timestamp': '2018-04-17T08:12:32.000Z',
            'some_property': 'somevalue',
        },
            {
                'timestamp': '2018-04-17T08:12:32.000Z',
                'some_property': None,
            },
            {
                'timestamp': None,
                'some_property': 'somevalue',
            }
        ]

        df = self.session.createDataFrame(data)

        missing = jlp.check_for_missing_columns(df)
        self.assertTrue(missing == [])

        missing = jlp.check_for_missing_columns(df, required_only=False)
        expected_missing_cols = list(self.schema['properties'].keys())
        expected_missing_cols.remove('timestamp')
        expected_missing_cols.remove('some_property')
        self.assertTrue(sorted(missing) == sorted(expected_missing_cols))

    def test_add_missing_columns(self):
        from pyspark.sql import functions as F
        self.schema['properties']['timestamp']['default'] = \
            '2018-01-01T00:00:00.000Z'
        self.schema['properties']['some_property']['default'] = 'DEFAULT'

        jlp = JSONLogSparkParser(self.schema)

        data = [{
            'timestamp': '2018-04-17T08:12:32.000Z',
            'some_property': 'somevalue',
        },
            {
                'timestamp': '2018-04-17T08:12:32.000Z',
                'some_property': None,
            },
            {
                'timestamp': None,
                'some_property': 'somevalue',
            }
        ]

        df = self.session.createDataFrame(data)
        missing = jlp.check_for_missing_columns(df, required_only=False)
        mdf = jlp.add_missing_columns(df, missing)
        expected_df = df
        for c in missing:
            expected_df = expected_df.withColumn(c, F.lit(None))

        expected_df = self.fix_schema(expected_df, mdf, mdf.columns)

        self.assertDataFrameEqual(mdf, expected_df)

    def test_drop_if_missing_filter(self):
        jlp = JSONLogSparkParser(self.schema)

        data = [
            {
                'timestamp': '2018-04-17T08:12:32.000Z',
                'some_property': 'somevalue',
            },
            {
                'timestamp': '2018-04-17T08:12:32.000Z',
                'some_property': None,
            },
            {
                'timestamp': None,
                'some_property': 'somevalue',
            }
        ]

        df = self.session.createDataFrame(data)
        jlp.drop_row_if_missing = ['timestamp', 'some_property']

        actual_filter = jlp.drop_if_missing_filter()

        filtered_df = df.select('*').where(actual_filter)

        self.assertTrue(filtered_df.count() == 1)

        self.assertDataFrameEqual(
            filtered_df,
            df.select(filtered_df.columns).where(
                F.col('timestamp').isNotNull() &
                F.col('some_property').isNotNull()
            )
        )
