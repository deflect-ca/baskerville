from typing import T, Any, Mapping

from baskerville.spark import get_spark_session
from baskerville.util.enums import LabelEnum
from baskerville.util.helpers import class_from_str
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


def load_model_from_path(model_type, path):
    """
    Loads a spark ml model from a path
    :param str | AlgorithmEnum | ScalerEnum model_type: the string
    representation of the model import, e.g.
    `pyspark.ml.feature.StandardScaler`
    :param str path:
    :return:
    """
    return class_from_str(model_type).load(path)


def save_model(model, path, mode='overwrite'):
    """
    Save a Pyspark ML model to disk / hdfs
    :param Estimator model:
    :param str path:
    :param str mode:
    :return:
    """
    writer = model.write()
    if mode == 'overwrite':
        writer = writer.overwrite()
    else:
        writer = writer.option('mode', mode)
    writer.save(path)


def set_unknown_prediction(df, columns=('prediction', 'score')):
    """
    Sets the preset unknown value for prediction and score
    :param pyspark.sql.Dataframe df:
    :param columns:
    :return:
    """
    for c in columns:
        df = df.withColumn(c, F.lit(LabelEnum.unknown.value))
    return df
