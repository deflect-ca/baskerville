import itertools
import logging
import datetime
import os
import threading

from sqlalchemy.sql.functions import sysdate

from baskerville.db import set_up_db, get_jdbc_url
from baskerville.db.models import Attack, Attribute
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from baskerville.models.storage_io import StorageIO


class Labeler(object):

    def __init__(self,
                 db_config,
                 spark,
                 check_interval_in_seconds=120,
                 regular_traffic_before_attack_in_minutes=60,
                 load_from_storage=True,
                 logger=None,
                 storage_path=None,
                 folder_stream='stream',
                 folder_attacks='attacks'
                 ):
        super().__init__()
        self.db_config = db_config
        self.spark = spark
        self.folder_attacks = folder_attacks
        self.storage_path = storage_path
        self.check_interval_in_seconds = check_interval_in_seconds
        self.regular_traffic_before_attack_in_minutes = regular_traffic_before_attack_in_minutes
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('baskerville')
            self.logger.addHandler(logging.StreamHandler(sysdate.stdout))
        self.thread = None
        self.kill = threading.Event()
        self.load_from_storage = load_from_storage
        self.storage_io = StorageIO(
            storage_path=storage_path,
            spark=self.spark,
            logger=self.logger,
            subfolder=folder_stream
        ) if self.load_from_storage else None

    def start(self):
        if self.thread is not None:
            return

        self.thread = threading.Thread(target=self._run)
        self.thread.start()

    def stop(self):
        if self.thread is None:
            return
        self.kill.set()
        self.thread.join()

    def join(self):
        self.thread.join()

    def _load_dataset(self, query, engine):
        self.logger.info('Querying database...')
        self.logger.info(query)
        df = pd.read_sql(query, engine)
        self.logger.info(f'{len(df)} records retrieved.')
        num_ips = len(df['ip'].unique())
        self.logger.info(f'{num_ips} unique IPs')
        df = df.fillna(0)
        return df

    def _label_attack(self, attack, engine):
        self.logger.info(f'Labeling incident id = {attack.id}, target={attack.target} ...')

        regular_start = attack.start - datetime.timedelta(minutes=self.regular_traffic_before_attack_in_minutes)
        df_regular = self._load_dataset(f'select distinct ip from request_sets where target=\'{attack.target}\' and '
                                        f'stop >= \'{regular_start.strftime("%Y-%m-%d %H:%M:%S")}Z\' and '
                                        f'stop < \'{attack.start.strftime("%Y-%m-%d %H:%M:%S")}Z\'', engine)
        self.logger.info(f'unique regular = {len(df_regular)}')

        df_attack = self._load_dataset(f'select distinct ip from request_sets where target=\'{attack.target}\' and '
                                       f'stop >= \'{attack.start.strftime("%Y-%m-%d %H:%M:%S")}Z\' and '
                                       f'stop < \'{attack.stop.strftime("%Y-%m-%d %H:%M:%S")}Z\'', engine)

        self.logger.info(f'unique attackers = {len(df_attack)}')

        df_attack = df_attack.merge(df_regular, on=['ip'], how='outer', indicator=True)
        df_attack = df_attack[df_attack['_merge'] == 'left_only']
        self.logger.info(f'df_attack after merge = {len(df_attack)}')

        # update ips in the database
        session, engine = set_up_db(self.db_config.__dict__)
        try:
            existing_attack = session.query(Attack).get(attack.id)
            if not existing_attack:
                self.logger.error(f'Attack {attack.id} is not in the database.')
                return

            attributes = []
            for ip in df_attack['ip']:
                a = Attribute()
                a.value = ip
                a.attacks.append(existing_attack)
                attributes.append(a)

            session.add_all(attributes)
            existing_attack.labeled = True
            session.commit()
            self.logger.info(f'Incident labeled, target={attack.target}, id = {attack.id}, num ips = {len(df_attack)}')

        except Exception as e:
            self.logger.error(str(e))
            return

        finally:
            session.close()
            engine.dispose()

    def _load_request_sets(self, query):
        self.logger.info('loading from postgres...')
        db_url = get_jdbc_url(self.db_config)
        conn_properties = {
            'user': self.db_config.user,
            'password': self.db_config.password,
            'driver': 'org.postgresql.Driver',
        }
        df = self.spark.read.jdbc(
            url=db_url,
            table=query,
            properties=conn_properties
        )
        json_schema = self.spark.read.json(df.limit(1).rdd.map(lambda row: row.features)).schema
        df = df.withColumn('features1', F.from_json('features', json_schema))
        df = df.withColumn('features', F.create_map(
            *list(itertools.chain(*[(F.lit(f), F.col('features1').getItem(f)) for f in json_schema.__dict__['names']])
                  ))).drop('features1')

        return df

    def _save_df_to_s3(self, df, attack_id):
        self.logger.info('writing to parquet...')
        df.repartition(10).write.parquet(os.path.join(self.storage_path, self.folder_attacks, f'{attack_id}'))

    def _save_attack(self, attack):
        self.logger.info(f'Saving attack {attack.id} to s3...')
        attack_ips = [a.value for a in attack.attributes]
        attack_ips = self.spark.createDataFrame(data=[[a] for a in set(attack_ips)],
                                                schema=StructType([StructField("ip_attacker", StringType())]))

        regular_start = attack.start - datetime.timedelta(minutes=self.regular_traffic_before_attack_in_minutes)

        if self.load_from_storage:
            self.logger.info(f'Loading from storate target={attack.target}, from {regular_start} to {attack.stop}')
            df = self.storage_io.load(host=attack.target, start=regular_start, stop=attack.stop)
        else:
            query = f'(select * from request_sets where ' \
                    f'target = \'{attack.target}\' and ' \
                    f'stop >= \'{regular_start.strftime("%Y-%m-%d %H:%M:%S")}Z\'::timestamp ' \
                    f'and stop < \'{attack.stop.strftime("%Y-%m-%d %H:%M:%S")}Z\'::timestamp) as attack1 '
            self.logger.info(query)
            df = self._load_request_sets(query)

        if not df or len(df.head(1)) == 0:
            print(f'Skipping attack {attack.id}, no records found.')
            return

        self.logger.info(f'Total records = {df.count()}')
        self.logger.info(f'Num attacker ips={attack_ips.count()}')
        unique_attackers = attack_ips.groupBy('ip_attacker').count().count()
        self.logger.info(f'Unique attackers={unique_attackers}')

        self.logger.info('Joining the labels...')
        df = df.join(attack_ips, F.col('ip') == F.col('ip_attacker'), how='left')
        self.logger.info(f'After join = {df.count()}')
        df = df.withColumn('label', F.when(F.col('ip_attacker').isNull(), 0.0).otherwise(1.0))

        num_positives = df.where(F.col('label') == 1).count()
        num_negatives = df.where(F.col('label') == 0).count()
        self.logger.info(f'Positives= {num_positives}')
        self.logger.info(f'Negatives= {num_negatives}')

        num_positives_unique = df.where(F.col('label') == 1).groupBy('ip').count().count()
        num_negatives_unique = df.where(F.col('label') == 0).groupBy('ip').count().count()
        self.logger.info(f'Positive uniques = {num_positives_unique}')
        self.logger.info(f'Negatives unique = {num_negatives_unique}')

        self._save_df_to_s3(df, attack.id)

        self.logger.info('Updating saved_to_cloud column...')
        session, engine = set_up_db(self.db_config.__dict__)
        try:
            existing_attack = session.query(Attack).get(attack.id)
            if not existing_attack:
                self.logger.error(f'Attack {attack.id} is not in the database.')
                return
            existing_attack.saved_in_cloud = True
            session.commit()
            self.logger.info(f'Incident saved in s3, target={attack.target}, id = {attack.id}')

        except Exception as e:
            self.logger.error(str(e))
            return

        finally:
            session.close()
            engine.dispose()

    def _label(self):
        try:
            session, engine = set_up_db(self.db_config.__dict__)
        except Exception as e:
            self.logger.error(str(e))
            return

        try:
            attacks = session.query(Attack).filter(Attack.approved.is_(True) & (~Attack.labeled.is_(True)))
            for attack in attacks:
                self._label_attack(attack, engine)

        except Exception as e:
            self.logger.error(str(e))
        finally:
            session.close()
            engine.dispose()

    def _save(self):
        try:
            session, engine = set_up_db(self.db_config.__dict__)
        except Exception as e:
            self.logger.error(str(e))
            return

        try:
            attacks = session.query(Attack).filter(Attack.approved.is_(True)
                                                   & Attack.labeled.is_(True)
                                                   & (~Attack.saved_in_cloud.is_(True)))
            for attack in attacks:
                self._save_attack(attack)

        except Exception as e:
            self.logger.error(str(e))
        finally:
            session.close()
            engine.dispose()

    def _run(self):
        self.logger.info('Starting labeler...')
        while True:
            self._label()
            self._save()
            is_killed = self.kill.wait(self.check_interval_in_seconds)
            if is_killed:
                break
