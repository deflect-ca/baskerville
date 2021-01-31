import os, sys
import time
import itertools

src_dir = os.environ['BASKERVILLE_ROOT'] + '/src'
if not os.path.lexists(src_dir):
    raise RuntimeError('Baskerville source dir does not exist!')
module_path = os.path.abspath(os.path.join(src_dir))
if module_path not in sys.path:
    sys.path.append(module_path)
print(src_dir)

from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from baskerville.db import get_jdbc_url

from baskerville.db.models import Attack
from baskerville.util.helpers import parse_config
from baskerville.models.config import BaskervilleConfig
from baskerville.util.baskerville_tools import BaskervilleDBTools
from baskerville.models.anomaly_model import AnomalyModel

configuration = 'training.yaml'
# num_hours_for_normal_traffic = 12
os.environ['TZ'] = 'UTC'
time.tzset()


def set_up_spark():
    conf = SparkConf()

    conf.set('spark.driver.host', 'localhost')
    conf.set('spark.master', 'local[10]')

    conf.set("spark.hadoop.dfs.client.use.datanode.hostname", True)
    conf.set('spark.jars',
             os.environ['BASKERVILLE_ROOT'] + '/data/jars/postgresql-42.2.4.jar')
    conf.set('spark.executor.memory', '40G')
    conf.set('spark.memory.offHeap.enabled', 'true')
    conf.set('spark.memory.offHeap.size', '35G')

    return SparkSession.builder.config(conf=conf).appName(
        "NotebookModelEvaluation"
    ).getOrCreate()


def get_baskerville_config():
    conf_path = os.environ['BASKERVILLE_ROOT'] + f'/conf/{configuration}'
    config = parse_config(path=conf_path)
    return BaskervilleConfig(config).validate()


def load_dataset(query, spark, db_config):
    db_url = get_jdbc_url(db_config)
    conn_properties = {
        'user': db_config.user,
        'password': db_config.password,
        'driver': 'org.postgresql.Driver',
    }
    df = spark.read.jdbc(
        url=db_url,
        table=query,
        properties=conn_properties
    )
    json_schema = spark.read.json(
        df.limit(1).rdd.map(lambda row: row.features)).schema

    df = df.withColumn('features', F.from_json('features', json_schema))
    df = df.withColumn('features', F.create_map(
        *list(itertools.chain(
            *[(F.lit(f), F.col('features').getItem(f)) for f in
              json_schema.__dict__['names']])
        )))
    return df


def evaluate_model(models, spark, db_config):
    config.database.maintenance = None
    db_tools = BaskervilleDBTools(db_config)
    db_tools.connect_to_db()

    attacks = db_tools.session.query(Attack).all()
    result = []
    for attack in attacks:

        print(
            f'Processing attack {attack.id}: target={attack.target}, start={attack.start}, stop={attack.stop}')

        attack_ips = [a.value for a in attack.attributes]

        attack_ips = spark.createDataFrame(data=[[a] for a in attack_ips],
                                           schema=StructType([StructField(
                                               "ip_attacker", StringType())]))

        print('Querying database...')
        query = f'(select ip, target, created_at, features, stop from request_sets where '
        f'stop > \'{attack.start.strftime("%Y-%m-%d %H:%M:%S")}Z\' and stop < \'{attack.stop.strftime("%Y-%m-%d %H:%M:%S")}Z\') as attack1 '

        rs = load_dataset(query, spark, db_config)
        num_records = rs.count()
        print(f'Loaded {num_records} records')
        if num_records == 0:
            print(f'Skipping attack {attack.id}, no records found.')
            continue

        print('Joining the labels...')
        rs = rs.join(attack_ips, rs.ip == attack_ips.ip_attacker, how='left')
        rs = rs.withColumn('label', F.when(F.col('ip_attacker').isNull(),
                                           0.0).otherwise(1.0))

        aucs = []
        for model in models:
            print('Predicting...')
            rs1 = model.predict(rs)
            print('Calculating metrics...')
            metrics = BinaryClassificationMetrics(
                rs1.select(['score', 'label']).rdd)
            auc = metrics.areaUnderPR
            print(f'Area under PR curve = {auc}')
            aucs.append(auc)
        result.append((attack, aucs))

    db_tools.disconnect_from_db()

    return result


if __name__ == '__main__':
    spark = set_up_spark()
    model_0 = AnomalyModel()
    model_0.load('hdfs:/...', spark)

    model_1 = AnomalyModel()
    model_1.load(
        'hdfs://hadoop-01:8020/prod/models/AnomalyModel__2020_10_08___15_32',
        spark
    )
    config = get_baskerville_config()
    metrics = evaluate_model([model_0, model_1], spark, config.database)
    print(metrics)
