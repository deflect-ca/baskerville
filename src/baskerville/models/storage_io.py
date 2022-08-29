# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
import random
from datetime import timedelta

import os
from pyspark.sql import functions as F

from baskerville.util.file_manager import FileManager


class StorageIO(object):

    def __init__(self, storage_path, spark, partitions=10, batch_in_minutes=20, logger=None, subfolder='stream'):
        self.spark = spark
        self.storage_path = storage_path
        self.batch_in_minutes = batch_in_minutes
        self.partitions = partitions
        self.batch = None
        self.prev_minutes = None
        self.prev_time = None
        self.logger = logger
        self.subfolder = subfolder

    def save(self, df, timestamp):
        # saving in  batches to s3, i.e 20 minutes, 40 minutes, 60 minutes, ...
        current_minutes = timestamp.minute // self.batch_in_minutes * self.batch_in_minutes

        if self.prev_minutes is None:
            self.prev_minutes = current_minutes
            self.prev_time = timestamp

        if current_minutes == self.prev_minutes:
            self.logger.info(f'appending chunk {timestamp} to {self.prev_minutes}')
            # self.logger.info(f'chunk count = {df.count()}')

            if not self.batch:
                self.logger.info('initial batch...')
                self.batch = df
            else:
                # self.logger.info(f'before union = {self.batch.count()}')
                self.batch = self.batch.union(df)
                # self.logger.info(f'after union = {self.batch.count()}')

            #self.batch = df if self.batch is None else self.batch.union(df)

            return

        path = os.path.join(
            self.storage_path, self.subfolder,
            f'{self.prev_time.year}',
            f'{self.prev_time.month:02d}',
            f'{self.prev_time.day:02d}',
            f'{self.prev_time.hour:02d}',
            f'{self.prev_minutes:02d}')

        file_manager = FileManager(os.path.join(self.storage_path, self.subfolder), self.spark)
        file_manager.delete_path(path)

        self.logger.info(f'writing to parquet {path}...')
        self.batch.repartition(self.partitions).write.parquet(path)

        self.batch = df
        self.prev_minutes = current_minutes
        self.prev_time = timestamp

    def load(self, start, stop, host=None, load_one_random_batch_from_every_hour=False):
        minutes = []
        for i in range(60 // self.batch_in_minutes):
            minutes.append(i * self.batch_in_minutes)

        file_manager = FileManager(os.path.join(self.storage_path, self.subfolder), self.spark)

        hour_chunks = []
        current_time = start - timedelta(hours=1)
        while current_time < stop:
            hour_chunks.append(current_time)
            current_time += timedelta(hours=1)

        dataset = None
        for i in range(len(hour_chunks)):
            chunk = hour_chunks[i]
            path = os.path.join(
                self.storage_path, self.subfolder,
                f'{chunk.year}',
                f'{chunk.month:02d}',
                f'{chunk.day:02d}',
                f'{chunk.hour:02d}')
            if not file_manager.path_exists(path):
                continue

            if load_one_random_batch_from_every_hour:
                minutes_to_load = [minutes[random.randrange(0, len(minutes))]]
            else:
                minutes_to_load = minutes

            for m in minutes_to_load:
                path_minutes = os.path.join(path, f'{m:02d}')
                if not file_manager.path_exists(path_minutes):
                    continue
                self.logger.info(f'Reading from {path_minutes}')
                df = self.spark.read.parquet(path_minutes)

                self.logger.info('1st read')
                self.logger.info(df.count())
                df = df.filter(f'stop >= \'{start.strftime("%Y-%m-%d %H:%M:%S")}\' and stop < \'{stop.strftime("%Y-%m-%d %H:%M:%S")}\'')
                self.logger.info('after filter start/stop')
                self.logger.info(df[['stop']].show())
                self.logger.info(df.count())

                if host:
                    df = df.filter(F.col('target') == host)
                    self.logger.info('after filter host')
                    self.logger.info(df.count())

                if dataset is None:
                    dataset = df
                else:
                    # make sure we have exactly the same feature names before calling unionByName
                    origin_features = set(dataset.schema['features'].dataType.names)
                    chunk_features = set(df.schema['features'].dataType.names)
                    for f in origin_features - chunk_features:
                        df = df.withColumn('features', F.struct(F.col('features.*'), F.lit('0').alias(f)))
                    for f in chunk_features - origin_features:
                        dataset = dataset.withColumn('features', F.struct(F.col('features.*'), F.lit('0').alias(f)))

                    # reorder the features for union(). Note: unionByName() did not work as expected for nested struct
                    df = df.withColumn('features', F.struct(
                        [F.col(f'features.{f}') for f in dataset.schema['features'].dataType.names]))

                    dataset = dataset.union(df)

                self.logger.info('after union')
                self.logger.info(dataset.count())

        return dataset


