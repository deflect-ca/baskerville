import json
import os
from collections import OrderedDict

import pyspark
import _pickle as cPickle

from baskerville.models.config import EngineConfig, \
    DatabaseConfig, SparkConfig
from baskerville.spark import get_or_create_spark_session
from baskerville.spark.helpers import save_df_to_table, map_to_array, \
    reset_spark_storage
from baskerville.spark.schemas import get_models_schema
from baskerville.spark.udfs import to_dense_vector_udf
from baskerville.util.enums import Step
from baskerville.util.helpers import instantiate_from_str, get_model_path, \
    get_scaler_load_path, get_classifier_load_path, RANDOM_SEED
from dateutil.tz import tzutc

from baskerville.models.base import TrainingPipelineBase
import datetime
import numpy as np
import pandas as pd

from baskerville.util.baskerville_tools import BaskervilleDBTools
from pyspark.ml.feature import VectorAssembler
from sklearn.preprocessing import StandardScaler    # todo
from sklearn.ensemble import IsolationForest        # todo
from baskerville.db.models import Model
from sklearn.preprocessing import LabelBinarizer
import pyspark.sql.functions as F


class TrainingPipeline(TrainingPipelineBase):

    def __init__(
            self,
            db_conf,
            engine_conf,
            clean_up=True
    ):
        super(TrainingPipeline, self).__init__(
            db_conf, engine_conf, clean_up
        )
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
        self.model = None
        self.scaler = None
        self.host_encoder = None
        self.db_tools = None
        self.db_conf = db_conf
        self.remaining_steps = list(self.step_to_action.keys())

    def finish_up(self):
        if self.db_tools:
            self.db_tools.disconnect_from_db()

    def initialize(self):
        conf = self.db_conf
        conf.maintenance = None
        self.active_features = self.engine_conf.extra_features
        self.db_tools = BaskervilleDBTools(conf)
        self.db_tools.connect_to_db()

    def get_date_filter(self):
        training_days = self.training_conf.data_parameters.get("training_days")
        return f'created_at > CURRENT_DATE - INTERVAL \'{training_days} days\''

    def get_model_parameters(self):
        return dict({
            'verbose': 10,
            'n_jobs': self.training_conf.n_jobs,
        }, **self.training_conf.classifier_parameters or {})

    def get_query(self):
        limit = ''
        if self.engine_conf.training.max_number_of_records_to_read_from_db:
            limit = f'limit {self.engine_conf.training.max_number_of_records_to_read_from_db}'
        return F'select target, features from request_sets where {self.get_date_filter()} {limit}'

    def get_data(self):
        self.data = pd.read_sql(self.get_query(), self.db_tools.engine)
        self.logger.info(f'{len(self.data)} records retrieved.')
        self.logger.info(f'Unwrapping features...')
        if len(self.data) > 0:
            self.data[[*self.data['features'][0].keys()]] = self.data[
                'features'
            ].apply(pd.Series)
            self.data.drop('features', axis=1, inplace=True)
        self.logger.info(f'Unwrapping features complete.')

        return self.data

    def train(self):
        self.scaler = StandardScaler()
        x_train = self.scaler.fit_transform(
            self.data[self.active_features].values
        )

        if self.engine_conf.training.use_host_feature > 0:
            self.host_encoder = LabelBinarizer()
            self.host_encoder.fit(self.training_conf.host_feature)
            self.logger.info('Host feature one-hot encoding...')
            host_features = self.host_encoder.transform(self.data['target'])
            self.logger.info('Host feature one-hot encoding concatenating...')
            x_train = np.concatenate((x_train, host_features), axis=1)

        self.model = IsolationForest()
        self.model.set_params(**self.get_model_parameters())
        self.logger.info('Model.fit()...')
        self.model.fit(x_train)
        self.logger.info('Model.fit() done.')
        return self.model, self.scaler, self.host_encoder

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

    def save(self, recall=0, precision=0, f1_score=0):

        model = Model()
        model.created_at = datetime.datetime.now(tz=tzutc())
        model.features = self.active_features
        model.algorithm = self.training_conf.classifier
        model.scaler_type = self.training_conf.scaler
        model.parameters = str(self.get_model_parameters())
        model.recall = float(recall)
        model.precision = float(precision)
        model.f1_score = float(f1_score)
        model.classifier = cPickle.dumps(self.model)
        model.scaler = cPickle.dumps(self.scaler)
        model.n_training = 0
        model.n_testing = 0
        model.host_encoder = cPickle.dumps(self.host_encoder)
        model_dict = {}
        for k in list(model.__dict__)[1:]:
            model_dict[k] = getattr(model, k)

        # save to db
        self.db_tools.session.add(model)
        self.db_tools.session.commit()


class TrainingSparkMLPipeline(TrainingPipelineBase):
    """
    Training Pipeline for the Spark ML Estimators
    todo: use a pipeline for the steps (scaling etc)
    """
    classifier: pyspark.ml.wrapper.JavaEstimator
    classifier_model: object
    scaler: pyspark.ml.feature.StandardScaler
    scaler_model: object
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
        super().__init__(db_conf, engine_conf, spark_conf, clean_up)
        self.logger.debug(f'{self.__class__.__name__} initiated')
        self.columns_to_keep = [
            'ip', 'target', 'created_at', 'features',
        ]

        self.model_path = get_model_path(self.engine_conf.storage_path, self.__class__.__name__)

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
        self.fit_params = {}
        self.conn_properties = {
                'user': self.db_conf.user,
                'password': self.db_conf.password,
                'driver': self.spark_conf.db_driver,
            }

        self.remaining_steps = list(self.step_to_action.keys())
        if self.training_conf.threshold:
            self.fit_params = {'threshold': self.training_conf.threshold}

    def initialize(self):
        """
        Get a spark session
        Create the classifier instance
        Create the scaler instance
        Set the appropriate parameters as set up in configuration
        :return:
        """
        self.spark = get_or_create_spark_session(self.spark_conf)
        self.feature_manager.initialize(None)
        self.classifier = instantiate_from_str(self.training_conf.classifier)
        self.scaler = instantiate_from_str(self.training_conf.scaler)

        self.classifier.setParams(
            **self.engine_conf.training.classifier_parameters
        )
        self.scaler.setParams(
            **self.engine_conf.training.scaler_parameters
        )
        self.classifier.setSeed(RANDOM_SEED)
        # self.scaler.setSeed(RANDOM_SEED)

    def get_data(self):
        """
        Load the data from the database into a dataframe and do the necessary
        transformations to get the features as a list \
        :return:
        """
        self.data = self.load().persist(self.spark_conf.storage_level)

        # since features are stored as json, we need to expand them to create
        # vectors
        json_schema = self.spark.read.json(
            self.data.limit(1).rdd.map(lambda row: row.features)
        ).schema
        self.data = self.data.withColumn(
            'features',
            F.from_json('features', json_schema)
        )

        # get the active feature names and transform the features to list
        self.active_features = json_schema.fieldNames()
        data = map_to_array(
            self.data,
            'features',
            'features',
            self.active_features
        ).persist(self.spark_conf.storage_level)
        self.data.unpersist()
        self.data = data
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
        # currently does not work with IForest:
        # https://github.com/titicaca/spark-iforest/issues/24
        # assembler = VectorAssembler(
        #     inputCols=self.active_features[:2],
        #     outputCol="vectorized_features"
        # )
        # self.data = assembler.transform(self.data)

        self.data = self.data.withColumnRenamed(
            'features', 'vectorized_features'
        ).withColumn(
            'vectorized_features',
            to_dense_vector_udf('vectorized_features')
        )

        self.scaler.setInputCol('vectorized_features')
        self.scaler.setOutputCol('scaled_features')
        self.classifier.setParams(featuresCol='scaled_features')

        self.scaler_model = self.scaler.fit(self.data)
        self.data = self.scaler_model.transform(self.data).persist(
            self.spark_conf.storage_level
        )

        self.classifier_model = self.classifier.fit(self.data, self.fit_params)

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
        self.logger.info(
            f'Saving model (classifier, scaler) in: {self.model_path}'
        )
        self.classifier_model.write().overwrite().save(
            get_classifier_load_path(self.model_path)
        )
        self.scaler_model.write().overwrite().save(
            get_scaler_load_path(self.model_path)
        )
        data = [
            [
                self.active_features,
                self.training_conf.classifier,
                self.training_conf.scaler,
                json.dumps(self.training_conf.to_dict()),
                0.,
                0.,
                0.,
                bytearray(self.model_path.encode('utf8')),
                bytearray([]),
                self.training_row_n,
                self.testing_row_n,
                float(self.training_conf.threshold)
            ]
        ]
        self.db_conf.conn_str = self.db_url

        model_df = self.spark.createDataFrame(data, schema=get_models_schema())
        save_df_to_table(
            model_df,
            Model.__tablename__,
            self.db_conf.__dict__,
            self.spark_conf.storage_level
        )

    def get_bounds(self, from_date, to_date=None, field='created_at'):
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

    def load(self, extra_filters=None) -> pyspark.sql.DataFrame:
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
        if not bounds.min_id:
            raise RuntimeError(
                'No data to train. Please, check your training configuration'
            )
        q = f'(select id, {",".join(self.columns_to_keep)} ' \
            f'from request_sets where id >= {bounds.min_id}  ' \
            f'and id <= {bounds.max_id} and created_at >= \'{from_date}\' ' \
            f'and created_at <=\'{to_date}\') as request_sets'

        if not extra_filters:
            return self.spark.read.jdbc(
                url=self.db_url,
                table=q,
                numPartitions=int(self.spark.conf.get(
                    'spark.sql.shuffle.partitions'
                )) or os.cpu_count()*2,
                column='id',
                lowerBound=bounds.min_id,
                upperBound=bounds.max_id + 1,
                properties=self.conn_properties
            )
        raise NotImplementedError(f'No implementation for "extra_filters"')

    def finish_up(self):
        """
        Unpersist all
        :return:
        """
        reset_spark_storage()
