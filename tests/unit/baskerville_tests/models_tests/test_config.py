import unittest
from baskerville.models.config import Config


class TestConfig(unittest.TestCase):
    def setUp(self):

        self.db_test_conf = Config({'name': 'baskerville',
                                    'user': 'user'})
        self.es_test_conf = Config({'user': 'elastic',
                                    'password': 'changeme'})
        self.auto_conf = Config({'time_bucket': 120,
                                 'verbose': False})
        self.manual_conf = Config({'host': 'somehost',
                                   'start': '2018-01-01 00:00:00',
                                   'stop': '2018-01-02 00:00:00'})
        self.engine_test_conf = Config({'auto': self.auto_conf,
                                        'manual': self.manual_conf,
                                        'logpath': '/var/log/baskerville.log'})
        self.kafka_test_conf = Config({'url': '0.0.0.0:9092',
                                       'zookeeper': 'localhost:2181'})
        self.spark_test_conf = Config({'master': 'local',
                                       'parallelism': -1})
        self.test_config = Config({
            'database': self.db_test_conf,
            'elastic': self.es_test_conf,
            'engine': self.engine_test_conf,
            'kafka': self.kafka_test_conf,
            'spark': self.spark_test_conf
            })

    def test_validate(self):
        pass


class TestBaskervilleConfig(unittest.TestCase):
    def setUp(self):
        pass

    # def test_instance(self):
    #     raise NotImplementedError()
    #
    # def test_validate(self):
    #     raise NotImplementedError()


class TestEngineConfig(unittest.TestCase):
    def setUp(self):
        pass

    # def test_instance(self):
    #     raise NotImplementedError()
    #
    # def test_validate(self):
    #     raise NotImplementedError()


class TestAutoConfig(unittest.TestCase):
    def setUp(self):
        pass

    # def test_instance(self):
    #     raise NotImplementedError()
    #
    # def test_validate(self):
    #     raise NotImplementedError()


class TestManualConfig(unittest.TestCase):
    def setUp(self):
        pass

    # def test_instance(self):
    #     raise NotImplementedError()
    #
    # def test_validate(self):
    #     raise NotImplementedError()


class TestSimulationConfig(unittest.TestCase):
    def setUp(self):
        pass

    # def test_instance(self):
    #     raise NotImplementedError()
    #
    # def test_validate(self):
    #     raise NotImplementedError()


class TestElasticConfig(unittest.TestCase):
    def setUp(self):
        pass

    # def test_instance(self):
    #     raise NotImplementedError()
    #
    # def test_validate(self):
    #     raise NotImplementedError()


class TestDatabaseConfig(unittest.TestCase):
    def setUp(self):
        pass

    # def test_instance(self):
    #     raise NotImplementedError()
    #
    # def test_validate(self):
    #     raise NotImplementedError()


class TestKafkaConfig(unittest.TestCase):
    def setUp(self):
        pass

    # def test_instance(self):
    #     raise NotImplementedError()
    #
    # def test_validate(self):
    #     raise NotImplementedError()


class TestSparkConfig(unittest.TestCase):
    def setUp(self):
        pass

    # def test_instance(self):
    #     raise NotImplementedError()
    #
    # def test_validate(self):
    #     raise NotImplementedError()

