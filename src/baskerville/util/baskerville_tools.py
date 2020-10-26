# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from __future__ import print_function
import pickle
from baskerville.util.crypto import encrypt, decrypt
import os
from baskerville.db import set_up_db
from baskerville.db.models import RequestSet, Runtime, Model

# Utility class that holds commonly used functions.


class BaskervilleDBTools(object):
    def __init__(self, conf):
        self.conf = conf
        self.session = None
        self.engine = None
        self.db = None

    def connect_to_db(self):
        """
        This connection to the db will live for the live time of the
        bothound_tools instance and will be used to save data back to the db
        """

        self.session, self.engine = set_up_db(self.conf.__dict__)

    def create_runtime(
            self,
            start=None,
            stop=None,
            dt_bucket=None,
            target_site=None,
            file_name=None,
            processed=None,
            comment=None,
            conf=None
    ):
        """
        Create a record in runtimes table.
        """
        try:
            runtime = Runtime()
            runtime.id_client = conf.id_client
            runtime.start = start
            runtime.stop = stop
            runtime.target = target_site
            runtime.file_name = file_name
            runtime.dt_bucket = dt_bucket
            runtime.processed = processed
            runtime.comment = comment
            runtime.config = str(conf)

            self.session.add(runtime)
            self.session.commit()
            return runtime
        except Exception:
            self.session.rollback()
            raise

    def encrypt(self, data):
        """

        :param data:
        :return:
        """
        return encrypt(self.conf.database.encryption_key, data, b'')

    def decrypt(self, data, data_iv, data_tag):
        """

        :param data:
        :param data_iv:
        :param data_tag:
        :return:
        """
        return decrypt(
            self.conf.database.encryption_key, "", data_iv, data, data_tag
        )

    def get_request_sets(self):
        """
        :return: all the RequestSets
        :rtype: list[RequestSet]
        """
        return self.session.query(RequestSet).all()

    def get_latest_ml_model_from_db(self):
        return self.session.query(Model).order_by(Model.id.desc()).first()

    def get_ml_model_from_db(self, model_id):
        if model_id < 0:
            return self.get_latest_ml_model_from_db()

        return self.session.query(Model).filter(Model.id == model_id).first()

    def get_ml_model_from_file(self, model_path):

        with open(model_path, 'rb') as f:
            local_model = pickle.load(f)

        model = Model()
        for k, v in local_model.items():
            setattr(model, k, v)
        self.session.add(model)
        self.session.flush()
        self.session.commit()

        return model

    def get_ml_clf_from_file(self, model_dir, version_id):
        path_to_model = f'{model_dir}/model_version{version_id}.sav'
        if not os.path.lexists(path_to_model):
            clf = None
        else:
            clf = pickle.load(
                open(path_to_model, 'rb')
            )
        return clf

    def get_ml_scaler_from_file(self, model_dir, version_id):
        path_to_scaler = f'{model_dir}/scaler_version{version_id}.sav'
        if not os.path.lexists(path_to_scaler):
            scaler = None
        else:
            scaler = pickle.load(
                open(path_to_scaler, 'rb')
            )
        return scaler

    def disconnect_from_db(self):
        """
        Close connection to the database
        """
        # todo: make sure the underlying db connection is also closed properly
        # else use NullPool
        self.session.close()
        self.engine.dispose()
