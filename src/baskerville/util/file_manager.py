# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import os
import shutil
import tempfile
import json
import _pickle as cPickle

import errno


class FileManager(object):

    def __init__(self, path, spark_session=None):
        """
        FileManager supports both local and distributed file systems.

        :param path: the root path of the storage. It must start with 'hdfs://ip:port/' for HDFS
        :param spark_session: the spark session
        """
        super().__init__()
        self.spark_session = spark_session

        if path.startswith('hdfs://'):
            if not self.spark_session:
                raise RuntimeError(
                    'You must pass a valid spark session if you use distributed storage')

            # find the third occurrence of '/'
            slash = path.find('/', path.find('/', path.find('/') + 1) + 1)
            if slash == -1:
                raise RuntimeError(f'Path "{path}" must contain at least one folder. '
                                   f'For example "hdfs://xxx:8020/baskerville"). ')
            connection_URI = path[:slash]
            self.jvm_path_class = self.spark_session._sc._gateway.jvm.org.apache.hadoop.fs.Path
            self.jvm_file_system = self.spark_session._sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark_session._sc._gateway.jvm.java.net.URI(
                    connection_URI),
                self.spark_session._sc._jsc.hadoopConfiguration())
        else:
            self.jvm_path = None
            self.jvm_file_system = None

    def path_exists(self, path):
        if self.jvm_file_system:
            return self.jvm_file_system.exists(self.jvm_path_class(path))
        return os.path.exists(path)

    def delete_path(self, path):
        if self.jvm_file_system:
            return self.jvm_file_system.delete(self.jvm_path_class(path), True)

        try:
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
            return True
        except OSError:
            return False

    def rename_path(self, source, destination):
        if self.jvm_file_system:
            self.jvm_file_system.rename(self.jvm_path_class(source),
                                        self.jvm_path_class(destination))
            return
        os.rename(source, destination)

    def _save_to_local_file(self, file, value, format='json'):
        if format == 'json':
            json.dump(value, file)
        elif format == 'pickle':
            cPickle.dump(value, file)
        else:
            raise RuntimeError(f'Unsupported file format {format}')

    def save_to_file(self, value, file_name, format='json'):
        mode = 'w' if format == 'json' else 'wb'

        if not self.jvm_file_system:
            if not os.path.exists(os.path.dirname(file_name)):
                try:
                    os.makedirs(os.path.dirname(file_name))
                except OSError as exc:  # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise

            with open(file_name, mode=mode) as f:
                self._save_to_local_file(f, value, format=format)
            return

        with tempfile.NamedTemporaryFile(mode=mode) as f:
            self._save_to_local_file(f, value, format=format)
            f.flush()

            self.jvm_file_system.copyFromLocalFile(
                self.jvm_path_class(f.name),
                self.jvm_path_class(file_name))

    def _load_from_local_file(self, file, format='json'):
        if format == 'json':
            return json.load(file)
        elif format == 'pickle':
            return cPickle.load(file)
        else:
            raise RuntimeError(f'Unsupported file format {format}')

    def load_from_file(self, path, format='json'):
        mode = 'r' if format == 'json' else 'rb'

        if not self.jvm_file_system:
            with open(path, mode=mode) as f:
                return self._load_from_local_file(f, format=format)

        with tempfile.NamedTemporaryFile(mode=mode) as f:
            self.jvm_file_system.copyToLocalFile(
                self.jvm_path_class(path),
                self.jvm_path_class(f.name))
            return self._load_from_local_file(f, format=format)
