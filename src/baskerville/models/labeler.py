import itertools
import logging
import datetime
import os
import threading

from sklearn.ensemble import GradientBoostingClassifier
from sqlalchemy.sql.functions import sysdate

from baskerville.db import set_up_db, get_jdbc_url
from baskerville.db.models import Attack, Attribute
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, DoubleType


class Labeler(object):

    def __init__(self,
                 db_config,
                 spark,
                 s3_path,
                 check_interval_in_seconds=120,
                 regular_traffic_before_attack_in_minutes=60,
                 logger=None):
        super().__init__()
        self.db_config = db_config
        self.spark = spark
        self.s3_path = s3_path
        self.check_interval_in_seconds = check_interval_in_seconds
        self.regular_traffic_before_attack_in_minutes = regular_traffic_before_attack_in_minutes
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('baskerville')
            self.logger.addHandler(logging.StreamHandler(sysdate.stdout))
        self.thread = None
        self.kill = threading.Event()

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
        self.logger.info(f'Unwrapping features...')
        if len(df) > 0:
            ff = df['features'].apply(pd.Series).columns.to_list()
            df[ff] = df['features'].apply(pd.Series)
            df.drop('features', axis=1, inplace=True)
            df.drop('host', axis=1, inplace=True)
            # df.drop('country', axis=1, inplace=True)
            df.drop('request_total', axis=1, inplace=True)
            df.drop('minutes_total', axis=1, inplace=True)
        self.logger.info(f'Unwrapping features complete.')
        df = df.fillna(0)
        return df

    def _label_attack(self, attack, engine):
        self.logger.info(f'Labeling incident id = {attack.id}, target={attack.target} ...')

        regular_start = attack.start - datetime.timedelta(minutes=self.regular_traffic_before_attack_in_minutes)
        df_regular = self._load_dataset(f'select * from request_sets where target=\'{attack.target}\' and '
                                        f'stop >= \'{regular_start.strftime("%Y-%m-%d %H:%M:%S")}Z\' and '
                                        f'stop < \'{attack.start.strftime("%Y-%m-%d %H:%M:%S")}Z\'', engine)

        df_attack = self._load_dataset(f'select * from request_sets where target=\'{attack.target}\' and '
                                       f'stop >= \'{attack.start.strftime("%Y-%m-%d %H:%M:%S")}Z\' and '
                                       f'stop < \'{attack.stop.strftime("%Y-%m-%d %H:%M:%S")}Z\'', engine)

        regular_ips = df_regular[['ip']].drop_duplicates()
        df_attack = df_attack.merge(regular_ips[['ip']], on=['ip'], how='outer', indicator=True)
        df_attack = df_attack[df_attack['_merge'] == 'left_only']

        features = [
            'request_rate',
            'css_to_html_ratio',
            'image_to_html_ratio',
            'js_to_html_ratio',
            'path_depth_average',
            'path_depth_variance',
            'payload_size_average',
            'payload_size_log_average',
            'request_interval_average',
            'request_interval_variance',
            'response4xx_to_request_ratio',
            'top_page_to_request_ratio',
            'unique_path_rate',
            'unique_path_to_request_ratio',
            'unique_query_rate',
            'unique_query_to_unique_path_ratio',
            'unique_ua_rate'
        ]

        labels = np.ones(len(df_regular), dtype=int)
        labels = np.append(labels, np.zeros(len(df_attack), dtype=int))
        dataset = pd.concat([df_regular[features], df_attack[features]])

        # scaler = StandardScaler()
        # dataset = scaler.fit_transform(dataset[features].values)

        model = GradientBoostingClassifier(
            n_estimators=500, random_state=777,
            max_depth=12,
            max_features='auto',
            learning_rate=0.05)
        model.fit(dataset, labels)

        predictions = model.predict(df_attack[features])

        incident = df_attack[['ip']].copy()
        incident['predictions'] = predictions
        attackers_predicted = incident[incident['predictions'] == 0][['ip']].drop_duplicates()

        incident_ips = incident[['ip']].drop_duplicates()
        regular_ips = df_regular[['ip']].drop_duplicates()

        attackers_vs_regular_traffic = pd.merge(regular_ips, attackers_predicted, how='inner', on=['ip'])
        regulars_vs_incident = pd.merge(regular_ips, incident_ips, how='inner', on=['ip'])

        self.logger.info(f'Number of unique IPs in the incident = {len(incident_ips)}')
        self.logger.info(f'Number of predicted attackers = {len(attackers_predicted)}')
        self.logger.info(f'Number of predicted regulars = {len(incident_ips) - len(attackers_predicted)}')
        self.logger.info(
            f'Intersection predicted attackers in regular traffic = {len(attackers_vs_regular_traffic)}, {len(attackers_vs_regular_traffic) / len(regular_ips) * 100:2.1f}%')
        self.logger.info(f'Intersection regular traffic in incident = {len(regulars_vs_incident)}')

        # filter out the IPs from regular traffic
        attackers = pd.merge(attackers_predicted, regulars_vs_incident, how='outer', on=['ip'], indicator=True)
        attackers = attackers[attackers['_merge'] == 'left_only'][['ip']]

        self.logger.info(f'Final number of attackers IP: {len(attackers)}')

        # update ips in the database
        session, engine = set_up_db(self.db_config.__dict__)
        try:
            existing_attack = session.query(Attack).get(attack.id)
            if not existing_attack:
                self.logger.error(f'Attack {attack.id} is not in the database.')
                return

            attributes = []
            for ip in attackers['ip']:
                a = Attribute()
                a.value = ip
                a.attacks.append(existing_attack)
                attributes.append(a)

            session.add_all(attributes)
            existing_attack.labeled = True
            session.commit()
            self.logger.info(f'Incident labeled, target={attack.target}, id = {attack.id}, num ips = {len(attackers)}')

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
        df.repartition(10).write.parquet(os.path.join(self.s3_path, f'{attack_id}'))

    def _save_attack(self, attack):
        self.logger.info(f'Saving attack {attack.id} to s3...')
        attack_ips = [a.value for a in attack.attributes]
        attack_ips = self.spark.createDataFrame(data=[[a] for a in attack_ips],
                                                schema=StructType([StructField("ip_attacker", StringType())]))

        query = f'(select * from request_sets where ' \
                f'target = \'{attack.target}\' and ' \
                f'stop > \'{attack.start.strftime("%Y-%m-%d %H:%M:%S")}Z\'::timestamp and stop < \'{attack.stop.strftime("%Y-%m-%d %H:%M:%S")}Z\'::timestamp) as attack1 '
        df = self._load_request_sets(query)
        if len(df.head(1)) == 0:
            print(f'Skipping attack {attack.id}, no records found.')
            return

        self.logger.info('Joining the labels...')
        df = df.join(attack_ips, df.ip == attack_ips.ip_attacker, how='left')
        df = df.withColumn('label', F.when(F.col('ip_attacker').isNull(), 0.0).otherwise(1.0))

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
            self.logger.info(f'Incident saved in s3, target={attack.target}, id = {attack.id}, path={self.s3_path}')

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
