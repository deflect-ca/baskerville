# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
import datetime
import os

from baskerville.util.file_manager import FileManager
from functools import reduce
from pyspark.sql import DataFrame


class SensitiveStorage(object):

    def __init__(self, path, spark_session, format='parquet', size=2, max_size=10):
        super().__init__()
        if max_size <= size:
            raise RuntimeError('Max size must be bigger than size.')

        self.path = os.path.join(path, 'sensitive_storage')
        self.spark_session = spark_session
        self.format = format
        self.size = size
        self.max_size = max_size
        self.index_file_path = os.path.join(self.path, 'index.txt')
        self.batches_path = os.path.join(self.path, 'batches')
        self.file_manager = FileManager(path=self.path, spark_session=spark_session)
        self.file_manager.delete_path(self.index_file_path)
        self.file_manager.delete_path(self.batches_path)
        self.file_manager.make_directory(self.path)
        self.file_manager.make_directory(self.batches_path)
        self._write_index([])

        self.prev_read_index = None
        self.prev_read_result = None

    @staticmethod
    def get_timestamp():
        ts = datetime.datetime.utcnow()
        return f'{ts.year:02}_{ts.month:02}_{ts.day:02}___{ts.hour:02}_{ts.minute:02}_{ts.second}_{ts.microsecond}'

    def _write_index(self, index):
        self.file_manager.save_to_file(index, self.index_file_path)

    def _read_index(self):
        return self.file_manager.load_from_file(self.index_file_path)

    def write(self, df):
        batch_file_name = os.path.join(self.batches_path, SensitiveStorage.get_timestamp())
        df.write.mode('overwrite').format(self.format).save(batch_file_name)

        # get the old index
        old_index = self._read_index()

        # copy only the latest batches to the new index
        i_last_batch_to_keep = len(old_index) - self.max_size + 1
        i_last_batch_to_keep = 0 if i_last_batch_to_keep < 0 else i_last_batch_to_keep
        new_index = old_index[i_last_batch_to_keep:len(old_index)]

        # add the new batch
        new_index.append(batch_file_name)

        # save the new index
        self._write_index(new_index)

        # delete expired batches
        for i in range(0, i_last_batch_to_keep):
            self.file_manager.delete_path(old_index[i])

    def read(self):
        index = self._read_index()
        if index == self.prev_read_index:
            return self.prev_read_result

        batches = []
        i_first_batch = len(index) - self.size
        i_first_batch = 0 if i_first_batch < 0 else i_first_batch

        for i in range(i_first_batch, len(index)):
            batches.append(self.spark_session.read.format(self.format).load(index[i]))

        result = reduce(DataFrame.unionAll, batches)

        self.prev_read_index = index
        self.prev_read_result = result

        return result
