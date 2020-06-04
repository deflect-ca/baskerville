# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


# from baskerville.models.config import BaskervilleConfig
# from baskerville.models.pipelines import KafkaPipeline
# from pyspark.sql import SQLContext
# from pyspark.sql.functions import udf
# from pyspark.sql.types import IntegerType
from sparktestingbase.testcase import SparkTestingBaseReuse
# import pyspark.sql.functions as F


class TestChallenge(SparkTestingBaseReuse):
    def test_get_challenge(self):
        pass
        # sc = SQLContext(self.sc)
        #
        # df = sc.createDataFrame(
        #     [
        #         ('host1.com', 1),
        #         ('host1.com', 0),
        #         ('host1.com', 0),
        #         ('host1.com', 0),
        #         ('host2.com', 1),
        #         ('host2.com', 1),
        #         ('host2.com', 0),
        #         ('host2.com', 0),
        #     ],
        #     ['target', 'prediction']
        # )
        #
        # conf = BaskervilleConfig({}).validate()
        #
        # pipeline = KafkaPipeline(conf.database, conf.engine, conf.kafka, conf.spark)
        # df = pipeline.get_challenges(df, 0.4)

        # def decision_function(num_normals, num_anomalies, threshold):
        #     if num_anomalies >= threshold * (num_anomalies + num_normals):
        #         return 1
        #     return 0
        #
        # decision_function_udf = udf(decision_function, IntegerType())
        #
        # df = df.select(['target', 'prediction'])\
        #     .groupBy(['target', 'prediction'])\
        #     .count()\
        #     .groupBy('target')\
        #     .pivot('prediction').agg(F.first('count'))\
        #     .withColumn('challenge', decision_function_udf(F.col('0'), F.col('1'), F.lit(t)))\
        #     .select(['target', 'challenge'])
