# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import json
import os
import unittest
from tempfile import TemporaryDirectory
from pyspark import SparkConf
from pyspark.sql import SparkSession
from os import listdir

from baskerville.models.sensitive_storage import SensitiveStorage


class TestFileManager(unittest.TestCase):
    def setUp(self):
        conf = SparkConf()
        self.spark = SparkSession \
            .builder.config(conf=conf) \
            .appName("TestingSensitiveStorage") \
            .getOrCreate()

    def test_read_write(self):
        with TemporaryDirectory() as temp_dir:
            storage = SensitiveStorage(path=temp_dir, spark_session=self.spark)

            num_batches = 30
            for i in range(num_batches):
                storage.write(self.spark.createDataFrame([[i]], ['id']))

            self.assertTrue(os.path.exists(os.path.join(temp_dir, storage.index_file_path)), 'Index file is missing')

            with open(storage.index_file_path) as fp:
                index = json.load(fp)

            self.assertTrue(index and len(index) == storage.max_size, 'Wrong index length')
            for file_name in index:
                self.assertTrue(os.path.exists(file_name), 'An indexed batch is missing')

            self.assertTrue(len(listdir(storage.batches_path)) == len(index), 'The total number of batches is wrong')

            df = storage.read()
            self.assertTrue(df.count() == storage.size, 'Wrong dataframe size after read()')

            result = df.collect()
            self.assertTrue(result[0][0] == num_batches - 2)
            self.assertTrue(result[1][0] == num_batches - 1)
