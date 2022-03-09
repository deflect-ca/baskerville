# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
import threading
import time
import pandas as pd

from baskerville.db import set_up_db


class DBReader(object):
    def __init__(self, db_config, query=None, logger=None, refresh_period_in_minutes=60):
        self.query = query
        self.db_config = db_config
        self.refresh_period_in_minutes = refresh_period_in_minutes
        self.last_timestamp = None
        self.logger = logger
        self.data = None
        self.fresh_data = None
        self.lock = threading.Lock()
        self.thread = None

    def set_query(self, query):
        self.query = query

    def _read_from_database(self):
        try:
            session, engine = set_up_db(self.db_config.__dict__)
        except Exception as e:
            if self.logger:
                self.logger.error(str(e))
            return None

        try:
            data = pd.read_sql(self.query, engine)
            with self.lock:
                self.fresh_data = data

        except Exception as e:
            print(str(e))
            if self.logger:
                self.logger.error(str(e))
        finally:
            session.close()
            engine.dispose()

    def _run(self):
        while True:
            if not self.last_timestamp or int(time.time() - self.last_timestamp) > self.refresh_period_in_minutes*60:
                self._read_from_database()
                self.last_timestamp = time.time()

    def _start(self):
        if not self.query:
            return
        if self.thread:
            return

        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def get(self):
        if not self.query:
            return None

        self._start()
        with self.lock:
            if self.fresh_data is not None:
                self.data = self.fresh_data.copy()
                self.fresh_data = None
        return self.data

