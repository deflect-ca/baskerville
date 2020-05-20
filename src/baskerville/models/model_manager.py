from baskerville.spark.helpers import set_unknown_prediction
from baskerville.util.enums import ANOMALY_MODEL_MANAGER
from baskerville.util.helpers import instantiate_from_str, get_logger
from pyspark.sql import functions as F, types as T


class ModelManager(object):
    def __init__(self, db_conf, engine_conf, spark_session=None, db_tools=None):
        self.db_conf = db_conf
        self.engine_conf = engine_conf
        self.ml_model = None
        self.can_predict = True
        self.db_tools = db_tools
        self.spark_session = spark_session
        self.anomaly_model_manager = None
        self.logger = get_logger(
            self.__class__.__name__,
            logging_level=self.engine_conf.log_level,
            output_file=self.engine_conf.logpath
        )

    def initialize(self, spark_session, db_tools):
        self.spark_session = spark_session
        self.db_tools = db_tools
        self.load()

    def load(self):
        """
        Loads the model from db if model_id is defined,
        :return:
        """
        if self.engine_conf.model_id:
            if self.engine_conf.model_id == -1:
                self.logger.debug('Loading latest Model from db')
                # get the latest models from db
                self.ml_model = self.db_tools.get_latest_ml_model_from_db()
            elif self.engine_conf.model_id > 0:
                self.ml_model = self.db_tools.get_ml_model_from_db(
                    self.engine_conf.model_id
                )
        elif self.engine_conf.model_path:
            self.ml_model = self.db_tools.get_ml_model_from_file(
                self.engine_conf.model_path
            )
        if self.ml_model:
            self.logger.debug(f'Loaded Model with id: {self.ml_model.id}')
            self.anomaly_model_manager = instantiate_from_str(
                ANOMALY_MODEL_MANAGER[self.ml_model.algorithm]
            )
            self.anomaly_model_manager.load(self.ml_model)
        else:
            self.logger.info('No Model loaded.')

        return self.ml_model

    def predict(self, df):
        """
        Use the anomaly model manager to predict on the dataframe or set
        the default values for prediction and score
        :param df:
        :return:
        """
        if self.can_predict and self.ml_model:
            df = self.anomaly_model_manager.predict(df)
        else:
            if not self.can_predict:
                self.logger.warn(
                    'Active features do not match model features, '
                    'skipping prediction'
                )
            elif not self.ml_model:
                self.logger.warn(
                    'No ml model specified, '
                    'skipping prediction'
                )
            df = set_unknown_prediction(
                df, columns=('score', 'prediction', 'threshold')
            ).withColumn(
                'prediction', F.col('prediction').cast(T.IntegerType())
            ).withColumn(
                'score', F.col('score').cast(T.FloatType())
            ).withColumn(
                'threshold', F.col('threshold').cast(T.FloatType())
            )

        return df
