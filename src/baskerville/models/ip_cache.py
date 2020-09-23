import datetime

from pyspark.sql import functions as F


class IPCache(object):

    def __init__(self, logger, ttl=60 * 60):
        super().__init__()

        self.ttl = ttl
        self.cache = None
        self.logger = logger

    def filter(self, df):
        if not self.cache:
            return df

        # return only the new comers from df
        original_count = df.count()
        df = df.join(self.cache[['ip']], on='ip', how='leftanti')
        new_count = df.count()
        self.logger.info(f'IP cache filters out {original_count - new_count} ips')
        return df

    def add(self, df):
        # add the new increment
        increment = df[['ip']].withColumn('ts', F.current_timestamp())
        if not self.cache:
            self.cache = increment
            return

        # remove expired records
        original_count = self.cache.count()
        now = datetime.datetime.utcnow()
        expire_date = now - datetime.timedelta(seconds=self.ttl)
        self.cache = self.cache.where(F.col('ts') >= expire_date)
        new_count = self.cache.count()
        self.logger.info(f'IP cache size after expiration = {new_count} ({new_count - original_count})')

        self.cache = self.cache.union(increment)
