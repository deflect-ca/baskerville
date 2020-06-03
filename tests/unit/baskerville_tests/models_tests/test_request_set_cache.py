from datetime import datetime, timedelta
from logging import Logger
from unittest import mock
from pyspark.sql import functions as F

from baskerville.models.request_set_cache import RequestSetSparkCache

from tests.unit.baskerville_tests.helpers.spark_testing_base import \
    SQLTestCaseLatestSpark


class TestRequestSetSparkCache(SQLTestCaseLatestSpark):

    def setUp(self):
        super().setUp()
        self.test_cache_config = {
            'db_url': 'db_url',
            'db_driver': 'db_driver',
            'user': 'user',
            'password': 'password',
        }
        self.test_table_name = 'test_table_name'
        self.test_columns_to_keep = ['1', '2']
        self.test_groupby_fields = ['a', 'b']

    def test_instance(self):
        rsc = RequestSetSparkCache(
            self.test_cache_config,
            self.test_table_name,
            self.test_columns_to_keep
        )
        self.assertTrue(hasattr(rsc, 'cache'))
        self.assertTrue(hasattr(rsc, 'cache_config'))
        self.assertTrue(hasattr(rsc, 'table_name'))
        self.assertTrue(hasattr(rsc, 'columns_to_keep'))
        self.assertTrue(hasattr(rsc, 'expire_if_longer_than'))
        self.assertTrue(hasattr(rsc, 'logger'))
        self.assertTrue(hasattr(rsc, 'session_getter'))
        self.assertTrue(hasattr(rsc, 'group_by_fields'))

        self.assertTrue(rsc.cache is None)
        self.assertTrue(isinstance(rsc.cache_config, dict))
        self.assertTrue(isinstance(rsc.table_name, str))
        self.assertTrue(isinstance(rsc.columns_to_keep, list))
        self.assertTrue(isinstance(rsc.expire_if_longer_than, int))
        self.assertTrue(isinstance(rsc.logger, Logger))
        self.assertTrue(callable(rsc.session_getter))
        self.assertTrue(isinstance(rsc.group_by_fields, tuple))

    def test__get_load_q(self):
        expected_q = f'''(SELECT *
                    from {self.test_table_name}
                    where id in (select max(id)
                    from {self.test_table_name}
                    group by {', '.join(self.test_groupby_fields)} )
                    ) as {self.test_table_name}'''

        rsc = RequestSetSparkCache(
            self.test_cache_config,
            self.test_table_name,
            self.test_columns_to_keep,
            group_by_fields=self.test_groupby_fields
        )

        q = rsc._get_load_q()

        self.assertEqual(q, expected_q)

    def test_load(self):
        update_date = datetime.utcnow()
        hosts = ('a', 'b')
        extra_filters = {}
        rsc = RequestSetSparkCache(
            self.test_cache_config,
            self.test_table_name,
            self.test_columns_to_keep,
            group_by_fields=self.test_groupby_fields
        )

        rsc._load = mock.MagicMock()
        persist = rsc._load.return_value.persist

        persist.assert_called_once()
        rsc._load.assert_called_once_with(
            update_date=None,
            hosts=None,
            extra_filters=None)

        rsc._load = mock.MagicMock()
        persist = rsc._load.return_value.persist
        persist.return_value = {}
        rsc.write = mock.MagicMock()
        returned_rsc = rsc.load(update_date, hosts, extra_filters)

        self.assertTrue(isinstance(returned_rsc.cache, dict))
        self.assertTrue(isinstance(rsc.cache, dict))

    @mock.patch('baskerville.models.request_set_cache.F.broadcast')
    def test__load(self, mock_broadcast):
        update_date = datetime.utcnow()
        hosts = ('a', 'b')
        extra_filters = (F.col('a') > 0)
        expected_where = str((
            ((F.col("updated_at") >= F.lit(update_date)) |
             (F.col("created_at") >= F.lit(update_date)))
            & extra_filters
        )._jc)
        rsc = RequestSetSparkCache(
            self.test_cache_config,
            self.test_table_name,
            self.test_columns_to_keep,
            group_by_fields=self.test_groupby_fields
        )

        rsc.session_getter = mock.MagicMock()
        format = rsc.session_getter.return_value.read.format
        options = format.return_value.options
        load = options.return_value.load
        where = load.return_value.where
        select = where.return_value.select
        join = select.return_value.join
        mock_broadcast.return_value = hosts

        format.assert_called_once_with('jdbc')
        options.assert_called_once_with(
            url=self.test_cache_config['db_url'],
            driver=self.test_cache_config['db_driver'],
            dbtable=rsc._get_load_q(),
            user=self.test_cache_config['user'],
            password=self.test_cache_config['password'],
            fetchsize=1000,
            max_connections=200,
        )
        select.assert_called_once_with(*self.test_columns_to_keep)
        load.assert_called_once()
        self.assertEqual(
            str(where.call_args_list[0][0][0]._jc),
            expected_where
        )
        join.assert_called_once_with(hosts, ['target'], 'leftsemi')
        # cache.assert_called_once()

    def test_append(self):
        rsc = RequestSetSparkCache(
            self.test_cache_config,
            self.test_table_name,
            self.test_columns_to_keep,
            group_by_fields=self.test_groupby_fields
        )
        rsc._RequestSetSparkCache__cache = mock.MagicMock()
        rsc._RequestSetSparkCache__cache.columns = []
        union = rsc._RequestSetSparkCache__cache.union
        union.return_value = '42'

        df = mock.MagicMock()
        df.select.return_value = 'hello world'
        returned_rsc = rsc.append(df)
        self.assertEqual(returned_rsc.cache, '42')
        self.assertEqual(rsc.cache, '42')

        df.select.assert_called_once_with(*self.test_columns_to_keep)
        union.assert_called_once_with('hello world')

    def test_refresh(self):
        update_date = datetime.utcnow()
        hosts = ['a', 'b']
        rsc = RequestSetSparkCache(
            self.test_cache_config,
            self.test_table_name,
            self.test_columns_to_keep,
            group_by_fields=self.test_groupby_fields
        )
        rsc._load = mock.MagicMock()
        rsc.append = mock.MagicMock()

        rsc._load.return_value = 42

        returned_rsc = rsc.refresh(update_date, hosts)
        self.assertTrue(isinstance(returned_rsc, RequestSetSparkCache))
        rsc._load.assert_called_once_with(
            extra_filters=None, update_date=update_date, hosts=hosts)
        rsc.append.assert_called_once_with(42)
        rsc.append.return_value.deduplicate.assert_called_once()

    def test_deduplicate(self):
        rsc = RequestSetSparkCache(
            self.test_cache_config,
            self.test_table_name,
            self.test_columns_to_keep,
            group_by_fields=self.test_groupby_fields
        )

        rsc._RequestSetSparkCache__cache = mock.MagicMock()
        drop_duplicates = rsc._RequestSetSparkCache__cache.dropDuplicates
        cache = drop_duplicates.return_value.cache
        cache.return_value = rsc._RequestSetSparkCache__cache

        rsc.deduplicate()

        drop_duplicates.assert_called_once()

    def test_alias(self):
        test_alias = 'test_alias'
        rsc = RequestSetSparkCache(
            self.test_cache_config,
            self.test_table_name,
            self.test_columns_to_keep,
            group_by_fields=self.test_groupby_fields
        )

        rsc._RequestSetSparkCache__cache = mock.MagicMock()
        alias = rsc._RequestSetSparkCache__cache.alias

        rsc.alias(test_alias)
        alias.assert_called_once_with(test_alias)

    def test_show(self):
        n = 10
        t = True
        rsc = RequestSetSparkCache(
            self.test_cache_config,
            self.test_table_name,
            self.test_columns_to_keep,
            group_by_fields=self.test_groupby_fields
        )
        rsc._RequestSetSparkCache__cache = mock.MagicMock()
        show = rsc._RequestSetSparkCache__cache.show

        rsc.show()
        show.assert_called_once_with(20, False)

        rsc._RequestSetSparkCache__cache = mock.MagicMock()
        show = rsc._RequestSetSparkCache__cache.show
        rsc.show(n, t)
        show.assert_called_once_with(n, t)

    def test_select(self):
        test_what = 42
        rsc = RequestSetSparkCache(
            self.test_cache_config,
            self.test_table_name,
            self.test_columns_to_keep,
            group_by_fields=self.test_groupby_fields
        )
        rsc._RequestSetSparkCache__cache = mock.MagicMock()
        select = rsc._RequestSetSparkCache__cache.select

        rsc.select(test_what)

        select.assert_called_once_with(test_what)

    def test_count(self):
        rsc = RequestSetSparkCache(
            self.test_cache_config,
            self.test_table_name,
            self.test_columns_to_keep,
            group_by_fields=self.test_groupby_fields
        )
        rsc._RequestSetSparkCache__cache = mock.MagicMock()
        count = rsc._RequestSetSparkCache__cache.count

        rsc.count()

        count.assert_called_once()

    def test_clean(self):
        now = datetime.utcnow()
        test_expire_longer_than = 60
        update_date = now - timedelta(seconds=test_expire_longer_than)
        filter_ = str((
            (F.col("updated_at") >= F.lit(update_date)) |
            (F.col("created_at") >= F.lit(update_date))
        )._jc)
        rsc = RequestSetSparkCache(
            self.test_cache_config,
            self.test_table_name,
            self.test_columns_to_keep,
            group_by_fields=self.test_groupby_fields
        )
        rsc._RequestSetSparkCache__cache = mock.MagicMock()
        select = rsc._RequestSetSparkCache__cache.select
        where = select.return_value.where

        rsc.clean(now, test_expire_longer_than)

        select.assert_called_once_with('*')
        # spark columns cannot be compared by equality
        self.assertEqual(
            str(where.call_args_list[0][0][0]._jc),
            filter_
        )

    def test_empty(self):
        rsc = RequestSetSparkCache(
            self.test_cache_config,
            self.test_table_name,
            self.test_columns_to_keep,
            group_by_fields=self.test_groupby_fields
        )
        rsc._RequestSetSparkCache__cache = mock.MagicMock()
        rsc.empty()
        self.assertTrue(rsc.cache is None)
