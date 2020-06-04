# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import traceback

from sqlalchemy import asc


def model_transfer(
        input_db_cfg, output_db_cfg, model_id=None, truncate_out_table=False
):
    """
    Transferring models between dbs. The databases can be located on the same
    server or on different servers. Use ssh tunneling for better performance
    in the second case.
    :param dict[str, T] input_db_cfg: the database configuration for the db to
    get data from
    input_db_cfg = {
        'name': 'baskerville_training',
        'user': 'postgres',
        'password': 'secret',
        'host': '127.0.0.1',
        'port': 5432,
        'type': 'postgres',
    }
    :param dict[str, T] output_db_cfg: the database configuration for the db to
    store data to, e.g.
    output_db_cfg = {
        'name': 'baskerville_production',
        'user': 'postgres',
        'password': 'secret',
        'host': '127.0.0.1',
        'port': 5432,
        'type': 'postgres',
    }
    :param int model_id: specify an id to get only one model
    :param bool truncate_out_table: Drop the models' data on the target
    database (True) or not (False) and restarts identity
    :return: None
    """
    from baskerville.models.config import DatabaseConfig
    from baskerville.db import set_up_db
    from baskerville.db.models import Model

    in_db_cfg = DatabaseConfig(input_db_cfg)
    out_db_cfg = DatabaseConfig(output_db_cfg)

    in_session, _ = set_up_db(in_db_cfg.__dict__)
    out_session, _ = set_up_db(out_db_cfg.__dict__)

    if truncate_out_table:
        if input(
                'Are you sure you want to truncate table data?(y/n)'
        ) in ['Y', 'y']:
            print('Truncating data...')
            out_session.execute(
                f'TRUNCATE TABLE {Model.__tablename__} RESTART IDENTITY'
            )
            out_session.commit()
        else:
            print('Skipping data truncation...')

    models_in = in_session.query(Model)
    if model_id:
        models_in = models_in.filter_by(id=model_id)
    models_in = models_in.order_by(asc(Model.id)).all()

    print(f'{len(models_in)} models in in_db: {in_db_cfg.name}')

    try:
        for model in models_in:
            print(f'Getting model with id: {model.id}')
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
            model_out.request_sets = model.request_sets
            out_session.add(model_out)
            out_session.commit()
    except Exception:
        traceback.print_exc()
        out_session.rollback()


if __name__ == '__main__':
    input_db_cfg = {
        'name': 'baskerville_report',
        'user': 'postgres',
        'password': 'secret',
        'host': '127.0.0.1',
        'port': 5434,
        'type': 'postgres',
    }
    output_db_cfg = {
        'name': 'baskerville_test',
        'user': 'postgres',
        'password': 'secret',
        'host': '127.0.0.1',
        'port': 5432,
        'type': 'postgres',
    }
    model_transfer(input_db_cfg, output_db_cfg, truncate_out_table=True)
