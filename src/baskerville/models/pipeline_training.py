# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import os
from collections import OrderedDict
import json
import pyspark
from pyspark.sql.types import StructField, StringType

from baskerville.models.config import EngineConfig, DatabaseConfig, SparkConfig
from baskerville.spark import get_or_create_spark_session
from baskerville.spark.helpers import reset_spark_storage
from baskerville.util.enums import Step
from baskerville.util.helpers import instantiate_from_str, get_model_path
from baskerville.db.models import Model

from baskerville.models.base import PipelineBase
import datetime
from dateutil.tz import tzutc

from baskerville.util.baskerville_tools import BaskervilleDBTools

import pyspark.sql.functions as F


class TrainingPipeline(PipelineBase):
    """
    Training Pipeline
    """
    model: object
    evaluation_results: dict
    data: pyspark.sql.DataFrame
    spark: pyspark.sql.SparkSession

    def __init__(
            self,
            db_conf: DatabaseConfig,
            engine_conf: EngineConfig,
            spark_conf: SparkConfig,
            clean_up: bool = True
    ):
        super().__init__(db_conf, engine_conf, clean_up)
        self.data = None
        self.training_conf = self.engine_conf.training
        self.spark_conf = spark_conf

        self.logger.debug(f'{self.__class__.__name__} initiated')
        self.columns_to_keep = [
            'ip', 'target', 'created_at', 'features',
        ]

        self.step_to_action = OrderedDict(
            zip([
                Step.get_data,
                Step.train,
                Step.test,
                Step.evaluate,
                Step.save,
            ], [
                self.get_data,
                self.train,
                self.test,
                self.evaluate,
                self.save,
            ]))
        self.training_row_n = 0
        self.testing_row_n = 0
        self.db_tools = None
        self.conn_properties = {
            'user': self.db_conf.user,
            'password': self.db_conf.password,
            'driver': self.spark_conf.db_driver,
        }

        self.remaining_steps = list(self.step_to_action.keys())

    def initialize(self):
        """
        Get a spark session
        Create the model instance
        Set the appropriate parameters as set up in configuration
        :return:
        """
        self.spark = get_or_create_spark_session(self.spark_conf)
        self.model = instantiate_from_str(self.training_conf.model)
        params = self.engine_conf.training.model_parameters

        # convert a list of features to the dictionary supported by the model:
        # {
        #     'feature1': {'categorical': False},
        #     'feature2': {'categorical': True, 'string': True}
        # }
        model_features = {}
        for feature in params['features']:
            features_class = self.engine_conf.all_features[feature]
            model_features[feature] = {
                'categorical': features_class.is_categorical(),
                'string': features_class.spark_type() == StringType()
            }
        params['features'] = model_features

        self.model.set_params(**params)
        self.model.set_logger(self.logger)
        conf = self.db_conf
        conf.maintenance = None
        self.db_tools = BaskervilleDBTools(conf)
        self.db_tools.connect_to_db()

    def run(self, *args, **kwargs):
        super().run()

    def get_data(self):
        """
        Load the data from the database into a dataframe and do the necessary
        transformations to get the features as a list \
        :return:
        """
        self.data = self.load() #.persist(self.spark_conf.storage_level)

        if self.training_conf.max_samples_per_host:
            counts = self.data.groupby('target').count()
            counts = counts.withColumn('fraction', self.training_conf.max_samples_per_host / F.col('count'))
            fractions = dict(counts.select('target', 'fraction').collect())
            for key, value in fractions.items():
                if value > 1.0:
                    fractions[key] = 1.0
            self.data = self.data.sampleBy('target', fractions, 777)

        schema = self.spark.read.json(self.data.limit(1).rdd.map(lambda row: row.features)).schema
        for feature in self.model.features:
            if feature in schema.fieldNames():
                continue
            features_class = self.engine_conf.all_features[feature]
            schema.add(StructField(
                name=feature,
                dataType=features_class.spark_type(),
                nullable=True))

        self.data = self.data.withColumn(
            'features',
            F.from_json('features', schema)
        )

        self.training_row_n = self.data.count()
        self.logger.debug(f'Loaded #{self.training_row_n} of request sets...')

    def train(self):
        """
        Vectorize the features and train on the loaded data
        Todo: train-test split:
        # self.train_data, self.test_data = self.data.randomSplit(
        #   [0.9, 0.1], seed=RANDOM_SEED
        # )
        :return: None
        """
        self.model.train(self.data)
        #self.data.unpersist()

    def test(self):
        """
        # todo
        :return:
        """
        self.logger.debug('Testing: Coming soon-ish...')

    def evaluate(self):
        """
        # todo
        :return:
        """
        self.logger.debug('Evaluating: Coming soon-ish...')

    def save(self):
        """
        Save the models on disc and add a baskerville.db.Model in the database
        :return: None
        """
        model_path = get_model_path(
            self.engine_conf.storage_path, self.model.__class__.__name__)
        self.model.save(path=model_path, spark_session=self.spark)

        db_model = Model()
        db_model.created_at = datetime.datetime.now(tz=tzutc())
        db_model.algorithm = self.training_conf.model
        db_model.parameters = json.dumps(self.model.get_params())
        db_model.classifier = bytearray(model_path.encode('utf8'))

        # save to db
        self.db_tools.session.add(db_model)
        self.db_tools.session.commit()

    def get_bounds(self, from_date, to_date=None, field='stop'):
        """
        Get the lower and upper limit
        :param str from_date: lower date bound
        :param str to_date: upper date bound
        :param str field: date field
        :return:
        """
        where = f'{field}>=\'{from_date}\' '
        if to_date:
            where += f'AND {field}<=\'{to_date}\' '
        q = f"(select min(id) as min_id, " \
            f"max(id) as max_id, " \
            f"count(id) as rows " \
            f"from request_sets " \
            f"where {where}) as bounds"
        return self.spark.read.jdbc(
            url=self.db_url,
            table=q,
            properties=self.conn_properties
        )

    def load(self) -> pyspark.sql.DataFrame:
        """
        Loads the request_sets already in the database
        :return:
        :rtype: pyspark.sql.Dataframe
        """
        data_params = self.training_conf.data_parameters
        from_date = data_params.get('from_date')
        to_date = data_params.get('to_date')
        training_days = data_params.get('training_days')

        if training_days:
            to_date = datetime.datetime.utcnow()
            from_date = str(to_date - datetime.timedelta(
                days=training_days
            ))
            to_date = str(to_date)
        if not training_days and (not from_date or not to_date):
            raise ValueError(
                'Please specify either from-to dates or training days'
            )

        bounds = self.get_bounds(from_date, to_date).collect()[0]
        self.logger.debug(
            f'Fetching {bounds.rows} rows. '
            f'min: {bounds.min_id} max: {bounds.max_id}'
        )
        q = f'(select id, {",".join(self.columns_to_keep)} ' \
            f'from request_sets where id >= {bounds.min_id}  ' \
            f'and id <= {bounds.max_id} and stop >= \'{from_date}\' ' \
            f'and stop <=\'{to_date}\') as request_sets'

        return self.spark.read.jdbc(
            url=self.db_url,
            table=q,
            numPartitions=int(self.spark.conf.get(
                'spark.sql.shuffle.partitions'
            )) or os.cpu_count() * 2,
            column='id',
            lowerBound=bounds.min_id,
            upperBound=bounds.max_id + 1,
            properties=self.conn_properties
        )

    def finish_up(self):
        """
        Unpersist all
        :return:
        """
        reset_spark_storage()
        if self.db_tools:
            self.db_tools.disconnect_from_db()
