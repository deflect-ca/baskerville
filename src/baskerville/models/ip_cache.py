import datetime

from pyspark.sql import functions as F


class IPCache(object):

    def __init__(self, logger, ttl=60 * 60):
        super().__init__()

        self.ttl = ttl
        self.cache = None
        self.logger = logger

    def update(self, df):
        increment = df[['ip']].withColumn('ts', F.current_timestamp())

        if not self.cache:
            self.cache = increment
            return df

        # return only the new comers from df
        self.logger.info('IP cache joining...')
        original_count = df.count()
        df = df.join(self.cache[['ip']], on='ip', how='leftanti')
        new_count = df.count()
        self.logger.info(f'IP cache total:{self.cache.count()}, existing:{original_count - new_count}, new:{new_count}')

        # remove expired records
        self.logger.info('IP cache removing expired records...')
        original_count = self.cache.count()
        now = datetime.datetime.utcnow()
        expire_date = now - datetime.timedelta(seconds=self.ttl)
        self.cache = self.cache.where(F.col('ts') >= expire_date)
        new_count = self.cache.count()
        self.logger.info(f'IP cache size after expiration = {new_count} ({new_count - original_count})')

        # add the new increment
        self.logger.info('IP cache union...')
        self.cache = self.cache.union(increment)

        return df

