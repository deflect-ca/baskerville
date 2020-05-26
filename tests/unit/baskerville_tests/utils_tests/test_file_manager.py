import unittest
import tempfile
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

from baskerville.util.file_manager import FileManager


class TestFileManager(unittest.TestCase):

    def test_json(self):
        temp_dir = tempfile.gettempdir()
        fm = FileManager(path=temp_dir)

        some_dict = {'A': 777}
        file_name = os.path.join(temp_dir, 'file_manager_test.pickle')

        fm.save_to_file(value=some_dict, file_name=file_name, format='json')
        assert(os.path.exists(os.path.join(temp_dir, file_name)))

        res = fm.load_from_file(file_name)
        self.assertDictEqual(res, some_dict)

    def test_pickle(self):
        temp_dir = tempfile.gettempdir()
        fm = FileManager(path=temp_dir)

        some_dict = {'A': 777}
        file_name = os.path.join(temp_dir, 'file_manager_test.pickle')

        fm.save_to_file(value=some_dict, file_name=file_name, format='pickle')
        res = fm.load_from_file(file_name, format='pickle')
        self.assertDictEqual(res, some_dict)

    # HDFS tests are commented our since it should no be executed with all the unit tests
    # def test_json_hdfs(self):
    #     temp_dir = 'hdfs://hadoop-01:8020/anton2'
    #
    #     conf = SparkConf()
    #     conf.set('spark.hadoop.dfs.client.use.datanode.hostname', 'true')
    #
    #     spark = SparkSession \
    #         .builder.config(conf=conf) \
    #         .appName("aaa") \
    #         .getOrCreate()
    #
    #     fm = FileManager(path=temp_dir, spark_session=spark)
    #
    #     some_dict = {'A': 777}
    #     file_name = os.path.join(temp_dir, 'file_manager_test7.pickle')
    #
    #     fm.save_to_file(value=some_dict, file_name=file_name, format='json')
    #     res = fm.load_from_file(file_name, format='json')
    #     self.assertDictEqual(res, some_dict)
    #
    # def test_pickle_hdfs(self):
    #     temp_dir = 'hdfs://hadoop-01:8020/anton2'
    #
    #     conf = SparkConf()
    #     conf.set('spark.hadoop.dfs.client.use.datanode.hostname', 'true')
    #
    #     spark = SparkSession \
    #         .builder.config(conf=conf) \
    #         .appName("aaa") \
    #         .getOrCreate()
    #
    #     fm = FileManager(path=temp_dir, spark_session=spark)
    #     some_dict = {'A': 777}
    #     file_name = os.path.join(temp_dir, 'file_manager_test7.pickle')
    #
    #     fm.save_to_file(value=some_dict, file_name=file_name, format='pickle')
    #     res = fm.load_from_file(file_name, format='pickle')
    #     self.assertDictEqual(res, some_dict)
