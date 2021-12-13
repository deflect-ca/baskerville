import datetime

from elasticsearch import Elasticsearch


class ElasticWriter(object):

    def __init__(self, host, port, user, password):
        super().__init__()
        self.connection_string = f'https://{user}:{password}@{host}:{port}'
        self.es = None

    def __enter__(self):
        assert(self.es is None)
        self.es = Elasticsearch([self.connection_string])
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.es = None

    def write(self, index_name, doc_type, record):
        try:
            self.es.index(index=index_name, doc_type=doc_type, body=record)
        except Exception as ex:
            print('Error in ES indexing')
            print(str(ex))

    def write_challenge(self, ip, host, reason):
        ts = datetime.datetime.utcnow()
        index_name = f'banjax-{ts.year}.{ts.month:02}.{ts.day:02}'
        record = {
            'trigger': 'baskerville_challenge',
            'tags': [reason],
            'http_host': host,
            'client_ip': ip,
            '@timestamp': f'{ts.year}-{ts.month:02}-{ts.day:02}T{ts.hour:02}:{ts.minute:02}:{ts.second:02}.000Z'
        }
        self.write(index_name, doc_type='banjax', record=record)

