# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from typing import T, Any, Mapping

from baskerville.spark import get_spark_session
from baskerville.util.enums import LabelEnum
from baskerville.util.helpers import class_from_str, TimeBucket
from pyspark import AccumulatorParam
from pyspark import StorageLevel
from pyspark.sql import functions as F


# OFF-HEAP by default
StorageLevel.CUSTOM = StorageLevel(True, True, True, False, 1)


class StorageLevelFactory(object):
    _available_storage = {
        'OFF_HEAP': StorageLevel.OFF_HEAP,
        'DISK_ONLY': StorageLevel.DISK_ONLY,
        'DISK_ONLY_2': StorageLevel.DISK_ONLY_2,
        'MEMORY_ONLY': StorageLevel.MEMORY_ONLY,
        'MEMORY_ONLY_2': StorageLevel.MEMORY_ONLY_2,
        'MEMORY_AND_DISK': StorageLevel.MEMORY_AND_DISK,
        'MEMORY_AND_DISK_2': StorageLevel.MEMORY_AND_DISK_2,
        'MEMORY_ONLY_SER': StorageLevel.MEMORY_ONLY_SER,
        'MEMORY_ONLY_SER_2': StorageLevel.MEMORY_ONLY_SER_2,
        'MEMORY_AND_DISK_SER': StorageLevel.MEMORY_AND_DISK_SER,
        'MEMORY_AND_DISK_SER_2': StorageLevel.MEMORY_AND_DISK_SER_2,
    }

    @staticmethod
    def get_storage_level(level):
        if level in StorageLevelFactory._available_storage:
            StorageLevel.CUSTOM = StorageLevelFactory._available_storage[level]
            return StorageLevel.CUSTOM
        raise ValueError(f'{level} is not a valid value for StorageLevel')


class DictAccumulatorParam(AccumulatorParam):
    def __init__(self, zero_value):
        self.zero_value = zero_value

    def zero(self, value):
        return self.zero_value

    def addInPlace(self, value1: T, value2: Mapping[str, Any]) -> T:
        """Adds a term to this accumulator's value"""
        items = list(value2.items())
        # todo: for some reason we get a default dict as value at times
        if items and len(items) >= 1:
            for k, v in items:
                value1[k] += v
        else:
            value1['--unknown--'] += 1

        return value1


def col_to_json(df, col_name, out_column=None):
    """
    Convert the values of a column to Json.
    :param pyspark.Dataframe df:
    :param str col_name:
    :param str out_column:
    :return:
    :rtype: pyspark.Dataframe
    """
    if not out_column:
        out_column = col_name
    return df.withColumn(
        col_name,
        F.to_json(out_column)
    )


def save_df_to_table(
        df,
        table_name,
        db_config,
        storage_level='OFF_HEAP',
        json_cols=('features',),
        mode='append',
        db_driver='org.postgresql.Driver'
):
    """
    Save the dataframe to the database. Jsonify any columns that need to
    be
    :param pyspark.Dataframe df: the dataframe to save
    :param str table_name: where to save the dataframe
    :param db_config:
    :param storage_level:
    :param iterable json_cols: the name of the columns that need to be
    :param db_driver:
    jsonified, e.g. `features`
    :param str mode: save mode, 'append', 'overwrite' etc, see spark save
    options
    :return: None
    """
    if not isinstance(storage_level, StorageLevel):
        storage_level = StorageLevelFactory.get_storage_level(storage_level)
    df = df.persist(storage_level)
    for c in json_cols:
        df = col_to_json(df, c)
    df.write.format('jdbc').options(
        url=db_config['conn_str'],
        driver=db_driver,
        dbtable=table_name,
        user=db_config['user'],
        password=db_config['password'],
        stringtype='unspecified',
        batchsize=100000,
        max_connections=1250,
        rewriteBatchedStatements=True,
        reWriteBatchedInserts=True,
        useServerPrepStmts=False,
    ).mode(mode).save()


def map_to_array(df, map_col, array_col, map_keys):
    """
    Transforms map_col to array_col
    map_col:
    {
        'key1': 0.1
        'key2': 0.6
        'key3': 0.3
    }
    array_col:
    [ 0.1, 0.6, 0.3 ]
    :param pyspark.sql.Dataframe df:
    :param str map_col:
    :param str array_col:
    :param list map_keys:
    :return: pyspark.sql.Dataframe
    """
    return df.withColumn(
        array_col,
        F.array(
            *list(
                F.col(map_col).getItem(f)
                for f in map_keys
            )
        )
    )


def reset_spark_storage():
    """
    Cleans up cached / persisted rdds and tables
    :return: None
    """
    spark = get_spark_session()
    for (id, rdd_) in list(
            spark.sparkContext._jsc.getPersistentRDDs().items()
    )[:]:
        rdd_.unpersist()
        del rdd_

    spark.catalog.clearCache()
    # self.spark.sparkContext._jvm.System.gc()


def set_unknown_prediction(df, columns=('prediction', 'score', 'threshold')):
    """
    Sets the preset unknown value for prediction and score
    :param pyspark.sql.Dataframe df:
    :param columns:
    :return:
    """
    for c in columns:
        df = df.withColumn(c, F.lit(LabelEnum.unknown.value))
    return df


def load_test(df, load_test_num, storage_level):
    """
    If the user has set the load_test configuration, then multiply the
    traffic by `EngineConfig.load_test` times to do load testing.
    :return:
    """
    if load_test_num > 0:
        df = df.persist(storage_level)

        for i in range(load_test_num - 1):
            temp_df = df.withColumn(
                'client_ip', F.round(F.rand(42)).cast('string')
            )
            df = df.union(temp_df).persist(storage_level)

        print(f'---- Count after multiplication: {df.count()}')
    return df


def columns_to_dict(df, col_name, columns_to_gather):
    """
    Convert the columns to dictionary
    :param string column: the name of the column to use as output
    :return: None
    """
    import itertools
    from pyspark.sql import functions as F

    return df.withColumn(
        col_name,
        F.create_map(
            *list(
                itertools.chain(
                    *[
                        (F.lit(f), F.col(f))
                        for f in columns_to_gather
                    ]
                )
            ))
    )


def get_window(df, time_bucket: TimeBucket, storage_level: str):
    df = df.withColumn(
        'timestamp', F.col('@timestamp').cast('timestamp')
    )
    df = df.sort('timestamp')
    current_window_start = df.agg({"timestamp": "min"}).collect()[0][0]
    stop = df.agg({"timestamp": "max"}).collect()[0][0]
    window_df = None
    current_end = current_window_start + time_bucket.td

    while True:
        if window_df:
            window_df.unpersist(blocking=True)
            del window_df
        filter_ = (
                (F.col('timestamp') >= current_window_start) &
                (F.col('timestamp') < current_end)
        )
        window_df = df.where(filter_).persist(storage_level)
        if not window_df.rdd.isEmpty():
            print(f'# Request sets = {window_df.count()}')
            yield window_df
        else:
            print(f'Empty window df for {str(filter_._jc)}')
        current_window_start = current_window_start + time_bucket.td
        current_end = current_window_start + time_bucket.td
        if current_window_start >= stop:
            window_df.unpersist(blocking=True)
            del window_df
            break