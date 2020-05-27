import _pickle as cPickle
import os

from baskerville.models.config import DatabaseConfig, SparkConfig
from baskerville.db import set_up_db
from baskerville.db.models import Model
from baskerville.spark import get_or_create_spark_session
from baskerville.util.enums import AlgorithmEnum
from baskerville.util.helpers import get_default_data_path
from pyspark.ml.feature import StandardScalerModel


def pickle_model(model_id, db_config, out_path):
    """
    Pickle the baskerville.db.models.Model instance
    :param int model_id: which model to get from database
    :param dict db_config: the databse configuration
    :param str out_path: where to store the model
    :return:
    """
    db_cfg = DatabaseConfig(db_config)
    session, _ = set_up_db(db_cfg.__dict__)

    model = session.query(Model).filter_by(id=model_id).first()

    with open(out_path, 'wb') as out_f:
        cPickle.dump(model, out_f)


def import_pickled_model(db_config, model_path):
    """
    Loads the pickled model and imports it into the database
    :param dict db_config: the database configuration
    :return:
    """
    db_cfg = DatabaseConfig(db_config).validate()
    session, _ = set_up_db(db_cfg.__dict__)

    with open(model_path, 'rb') as f:
        model = cPickle.load(f)

        model_out = Model()
        model_out.scaler = model.scaler
        model_out.classifier = model.classifier
        model_out.features = model.features
        model_out.algorithm = model.algorithm
        model_out.analysis_notebook = model.analysis_notebook
        model_out.created_at = model.created_at
        model_out.f1_score = model.f1_score
        model_out.n_training = model.n_training
        model_out.n_testing = model.n_testing
        model_out.notes = model.notes
        model_out.parameters = model.parameters
        model_out.precision = model.precision
        model_out.recall = model.recall
        session.add(model_out)
        session.commit()


if __name__ == '__main__':
    db_cfg = {
        'name': 'baskerville_test',
        'user': 'postgres',
        'password': 'secret',
        'host': '127.0.0.1',
        'port': 5432,
        'type': 'postgres',
    }
    path = f'{get_default_data_path()}/model_10_baskerville_report.pkl'
    pickle_model(10, db_cfg, path)
    import_pickled_model(db_cfg, path)