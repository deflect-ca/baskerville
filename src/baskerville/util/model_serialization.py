import _pickle as cPickle
import os

from baskerville.models.config import DatabaseConfig, SparkConfig
from baskerville.db import set_up_db
from baskerville.db.models import Model
from baskerville.spark import get_or_create_spark_session
from baskerville.util.enums import AlgorithmEnum
from baskerville.util.helpers import get_default_data_path, \
    get_classifier_load_path, get_scaler_load_path
from pyspark.ml.feature import StandardScalerModel


def pickle_model(model_id, db_config, out_path, ml_model_out_path):
    """
    Pickle the baskerville.db.models.Model instance
    :param int model_id: which model to get from database
    :param dict db_config: the databse configuration
    :param str out_path: where to store the pickled db model
    :param str ml_model_out_path: where to store the actual ml model
    (and scaler)
    :return:
    """
    db_cfg = DatabaseConfig(db_config)
    session, _ = set_up_db(db_cfg.__dict__)

    model = session.query(Model).filter_by(id=model_id).first()

    with open(out_path, 'wb') as out_f:
        cPickle.dump(model, out_f)

    print(f'Pickled Model with id: {model_id} to {out_path}')
    if model.algorithm == AlgorithmEnum.isolation_forest_pyspark.value:
        model_path = model.classifier.decode('utf-8')
        if os.path.exists(model_path):
            from pyspark_iforest.ml.iforest import IForestModel
            spark = get_or_create_spark_session(
                SparkConfig(
                    {'jars': f'{get_default_data_path()}/jars/spark-iforest-2.4.0.jar'}
                ).validate())
            IForestModel.load(get_classifier_load_path(model_path)).write(
            ).overwrite().save(
                get_classifier_load_path(ml_model_out_path)
            )
            StandardScalerModel.load(get_scaler_load_path(model_path)).write(
            ).overwrite().save(
                get_scaler_load_path(ml_model_out_path)
            )
            print(f'Copied {model_path} to {ml_model_out_path}')


def import_pickled_model(db_config, model_path, current_storage_path):
    """
    Loads the pickled model and imports it into the database
    :param dict db_config: the database configuration
    :return:
    """
    db_cfg = DatabaseConfig(db_config).validate()
    session, _ = set_up_db(db_cfg.__dict__, partition=False)

    with open(model_path, 'rb') as f:
        model = cPickle.load(f)

        model_out = Model()
        model_out.scaler = model.scaler
        model_out.scaler_type = model.scaler_type
        model_out.host_encoder = model.host_encoder
        model_out.classifier = model.classifier
        model_out.threshold = model.threshold
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
        # todo: once the model changes are done:
        # model_out.scaler = bytearray(current_storage_path).encode('utf-8')
        #     restore_model_path(
        #         current_storage_path,
        #         model.scaler.decode('utf-8')).encode('utf-8')
        # )
        model_out.classifier = bytearray(current_storage_path.encode('utf-8'))
        #     bytearray(
        #     restore_model_path(
        #         current_storage_path,
        #         model.classifier.decode('utf-8')).encode('utf-8')
        # )
        session.add(model_out)
        session.commit()
    session.close()


def restore_model_path(current_storage_path, previous_path: str):
    import os

    filename = os.path.basename(previous_path)
    print('>> filename, path', filename, previous_path)
    if not os.path.isdir(previous_path):
        return os.path.join(current_storage_path, filename)
    return previous_path


if __name__ == '__main__':
    db_cfg = {
        'name': 'baskerville_test',
        'user': 'postgres',
        'password': 'secret',
        'host': '127.0.0.1',
        'port': 5432,
        'type': 'postgres',
    }
    db_model_path = f'{get_default_data_path()}/samples/sample_model'
    ml_model_path = f'{get_default_data_path()}/samples/test_model'
    pickle_model(56, db_cfg, db_model_path, ml_model_path)
    # import_pickled_model(db_cfg, db_model_path, ml_model_path)
