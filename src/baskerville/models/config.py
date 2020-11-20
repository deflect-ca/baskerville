# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import json
import os
import warnings
from datetime import datetime
from functools import wraps

import dateutil
from baskerville.util.enums import ModelEnum
from baskerville.util.helpers import get_logger, get_default_data_path, \
    SerializableMixin
from dateutil.tz import tzutc
from baskerville.features import FEATURES

logger = get_logger(__name__)


class ConfigError(Exception, SerializableMixin):
    """
    Custom Error to be used in the configuration error report
    """

    def __init__(self, message, fields, exception_type=ValueError):
        if isinstance(fields, str):
            fields = [fields]
        self.args = message, fields, exception_type.__name__

    def __str__(self):
        m, f, e = self.args
        return f'({e}, `field(s)`: {",".join(f)}){m} '


class ConfigErrorReport(object):
    """
    Singleton configuration report - gathers all the errors from all the
    configuration instances
    """

    def __init__(self):
        self._errors = []

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(ConfigErrorReport, cls).__new__(
                cls, *args, **kwargs
            )
        return cls._instance

    def __str__(self):
        for ce in self.errors:
            for m, fi, e in ce.args:
                logger.error(f'Configuration option "{fi}":{m} ({e})')

    @property
    def errors(self):
        return self._errors

    @property
    def serialized_errors(self):
        return [e.to_dict() for e in self.errors]

    def add_error(self, e):
        if isinstance(e, ConfigError):
            if e not in self._errors:
                self._errors.append(e)
            else:
                raise ValueError(f'Duplicate error {str(e)}')
        else:
            raise ValueError(
                f'Wrong type {type(e)}, only accepting {ConfigError.__name__}'
            )

    def report(self):
        r = ''

        for e in self.errors:
            r += str(e) + '\n'

        return r


def validate(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        self = args[0]
        if not self._is_validated:
            logger.error(
                f'{self.__class__.__name__} is not validated yet, '
                f'there may be inconsistencies.'
            )
        if self.errors:
            e = f'There are configuration errors: ' \
                f'{self._configuration_error_report.report()}'
            logger.error(e)
            raise ValueError(e)
        return fn(*args, **kwargs)
    return wrapper


class Config(SerializableMixin):
    """
    The base Config class
    """
    _remove = ('parent', 'errors', 'serialized_errors')

    def __init__(self, config_dict, parent=None):
        if config_dict:
            self.__dict__.update(**{
                k: v for k, v in self.__class__.__dict__.items()
                if '__' not in k and not callable(v)
            })
            self.__dict__.update(**config_dict)
        self.parent = parent
        self._is_validated = False
        self._is_valid = False
        self._configuration_error_report = ConfigErrorReport()

    @validate
    def __getattr__(self, attr, default=None):
        return self.__dict__.get(attr, default)

    @validate
    def __getitem__(self, attr):
        return self.__dict__[attr]

    def __str__(self):
        d = {}
        for attr in dir(self):
            v = getattr(self, attr)
            if '__' not in attr and not callable(v):
                if hasattr(v, '__dict__'):
                    d[attr] = v.__dict__
                else:
                    d[attr] = v

        return str(d)

    @property
    def errors(self):
        return self._configuration_error_report.errors

    @property
    def serialized_errors(self):
        return self._configuration_error_report.serialized_errors

    def add_error(self, e):
        self._configuration_error_report.add_error(e)

    def report(self):
        return self._configuration_error_report.report()

    @classmethod
    def set_logger(cls, log_level='INFO', log_path='baskerville.log'):
        global logger
        logger = get_logger(
            cls.__name__,
            logging_level=log_level,
            output_file=log_path
        )

    def validate(self):
        raise NotImplementedError('You called the base validate'
                                  'which is not implemented. Please, provide'
                                  'the implementation for it.')


class BaskervilleConfig(Config):
    """
    Top level configuration, holds and can validate all sub-configs:
    - database   : mandatory
    - engine     : mandatory
    - spark      : mandatory
    - elastic    : optional - depends on the chosen pipeline
    - misp       : optional - depends on the chosen pipeline
    - kafka      : optional - depends on the chosen pipeline

    """
    database = None
    elastic = None
    misp = None
    engine = None
    kafka = None
    spark = None

    def __init__(self, config):
        super(BaskervilleConfig, self).__init__(config)
        self.database = DatabaseConfig(self.database)
        self.engine = EngineConfig(self.engine, self)

        # depending on the pipeline these may or may not be present
        if self.elastic:
            self.elastic = ElasticConfig(self.elastic)
        if self.misp:
            self.misp = MispConfig(self.misp)
        if self.kafka:
            self.kafka = KafkaConfig(self.kafka)
        if self.spark:
            self.spark = SparkConfig(self.spark)

    def validate(self):
        logger.debug('Validating BaskervilleConfig...')
        # these two should always be present:
        self.database.validate()
        self.engine.validate()

        if self.elastic:
            self.elastic.validate()
        else:
            logger.debug('No elasticsearch config')
        if self.misp:
            self.misp.validate()
        else:
            logger.debug('No misp config')
        if self.kafka:
            self.kafka.validate()
        else:
            logger.debug('No kafka config')
        if self.spark:
            self.spark.validate()
        else:
            logger.debug('No spark config')

        self._is_validated = True
        self._is_valid = len(self.errors) == 0
        if not self._is_valid:
            raise ValueError(
                f'Invalid configuration: {self.report()}'
            )

        return self


class EngineConfig(Config):
    """
    The pipeline's configuration
    """
    es_log = None
    raw_log = None
    simulation = None
    datetime_format = '%Y-%m-%d %H:%M:%S'
    cache_path = None
    storage_path = None
    cache_expire_time = None
    cache_load_past = False
    cross_reference = False
    model_path = None
    model_id = None
    extra_features = None
    verbose = False
    metrics = None
    data_config = None
    logpath = 'baskerville.log'
    log_level = 'INFO'
    time_bucket = 120
    all_features = None
    load_test = False
    trigger_challenge = True
    anomaly_threshold = 0.45
    attack_threshold = 0.0
    minimum_number_attackers = 50
    challenge = 'ip'  # supported values : 'ip', 'host'
    training = None
    ttl = 500
    sliding_window = 360
    low_rate_attack_period = [600, 3600]
    low_rate_attack_total_request = [400, 2000]
    ip_cache_passed_challenge_ttl = 60*60*24  # 24h
    ip_cache_passed_challenge_size = 100000
    ip_cache_pending_ttl = 60*60*1  # 1h
    ip_cache_pending_size = 100000

    white_list_ips = []
    white_list_hosts = None
    banjax_sql_update_filter_minutes = 30
    banjax_num_fails_to_ban = 9
    register_banjax_metrics = False

    def __init__(self, config, parent=None):
        super(EngineConfig, self).__init__(config, parent)
        if self.es_log:
            self.es_log = ElkLogConfig(self.es_log, self)
        if self.raw_log:
            self.raw_log = RawlogConfig(self.raw_log, self)
        if self.simulation:
            self.simulation = SimulationConfig(self.simulation, self)
        if self.metrics:
            self.metrics = MetricsConfig(self.metrics)
        if self.data_config:
            self.data_config = DataParsingConfig(self.data_config)
        if self.training:
            self.training = TrainingConfig(self.training, self)
        self.all_features = dict(
            (f.feature_name_from_class(), f) for f in FEATURES
        )
        if not self.storage_path:
            self.storage_path = os.path.join(
                get_default_data_path(), 'storage')

    def validate(self):
        logger.debug('Validating EngineConfig...')
        if self.load_test:
            self.load_test = int(self.load_test)
        if not self.es_log and not self.raw_log and not self.training:
            logger.warn('es_log or raw_log config are not provided')
        if self.model_id and self.model_path:
            self.add_error(ConfigError(
                'Model version id and model path cannot both be specified.',
                ['model_id', 'model_path'],
            ))
        if not self.logpath:
            self.logpath = 'baskerville.log'
            warnings.warn('Path for the log file is not set, '
                          'setting it to {}'.format(self.logpath))
        if not self.log_level:
            self.log_level = 'ERROR'
            warnings.warn('No log level provided, default value set: ERROR')
        if self.es_log:
            self.es_log.validate()
        if self.raw_log:
            self.raw_log.validate()
        if self.simulation:
            self.simulation.validate()
        if self.data_config:
            self.data_config.validate()
        if self.metrics:
            self.metrics.validate()
        if self.training:
            self.training.validate()
        if not self.time_bucket:
            self.time_bucket = 120
            warnings.warn('Time bucket is set to the default 120 seconds.')
        if not self.time_bucket == 120:
            warnings.warn('Time bucket is NOT set to the default 120 seconds.')
        self.time_bucket = int(self.time_bucket)
        if not self.cache_expire_time:
            self.cache_expire_time = 86400
            warnings.warn(
                'Cache expire time is set to the default 86400 seconds (1 day)'
            )
        self.cache_expire_time = int(self.cache_expire_time)
        if self.extra_features:
            for f in self.extra_features:
                if f not in self.all_features:
                    self.add_error(
                        ConfigError(f'Unknown feature {f}.', [
                            'extra_features'])
                    )

        if len(self.low_rate_attack_period) != 2 or len(self.low_rate_attack_total_request) != 2:
            self.add_error(
                ConfigError('low_rate_attack_period and low_rate_attack_total_request must be lists of size 2')
            )

        self._is_validated = True

        return self


class ElkLogConfig(Config):
    hosts = None
    start = None
    stop = None
    batch_length = None
    save_logs_dir = None

    def __init__(self, config, parent=None):
        super(ElkLogConfig, self).__init__(config, parent)

    def validate(self):
        logger.debug('Validating EsConfig...')

        if not self.hosts:
            self.hosts = None
        if self.start and not isinstance(self.start, datetime):
            try:
                self.start = datetime.strptime(
                    self.start, self.parent.datetime_format
                ).replace(tzinfo=tzutc())
            except ValueError as e:
                self.add_error(ConfigError(e.args[0], 'start'))

        if self.stop and not isinstance(self.stop, datetime):
            try:
                self.stop = datetime.strptime(
                    self.stop, self.parent.datetime_format
                ).replace(tzinfo=tzutc())
            except ValueError as e:
                self.add_error(ConfigError(e.args[0], 'stop'))
        if self.batch_length:
            self.batch_length = float(self.batch_length)
        elif self.start and self.stop:
            self.batch_length = (self.stop - self.start).total_seconds() / 60.
        else:
            self.add_error(ConfigError(
                'Neither batch length nor stop point specified.',
                ['batch_length', 'stop'],
            ))
        if self.save_logs_dir:
            if not os.path.isdir(
                    '/'.join(self.save_logs_dir.rsplit('/')[:-1])):
                self.add_error(ConfigError(
                    'Path for save_logs_dir does not exist.',
                    ['save_logs_dir'],
                ))

        self._is_validated = True
        return self


class RawlogConfig(Config):
    paths = None

    def __init__(self, config, parent=None):
        super(RawlogConfig, self).__init__(config, parent)

    def validate(self):
        logger.debug('Validating RawlogConfig...')
        if self.paths:
            for log in self.paths:
                # allow * for spark
                if not os.path.isfile(log) and '*' not in log:
                    self.add_error(ConfigError(
                        f'File {log} does not exist.',
                        ['paths'],
                    ))
        else:
            self.paths = []

        self._is_validated = True
        return self


class SimulationConfig(Config):
    sleep = True
    verbose = False
    log_file = ''

    def __init__(self, config, parent=None):
        super(SimulationConfig, self).__init__(config, parent)

    def validate(self):
        logger.debug('Validating SimulationConfig...')
        if self.log_file:
            log = self.log_file.replace('*', '')
            if not os.path.isfile(log) and not os.path.isdir(log):
                self.errors.append(ConfigError(
                    f'Not a valid path: {self.log_file}.'
                    'A valid path to a log file(s) must be provided for '
                    'the simulation, '
                    'e.g. /path/to/baskerville/data/samples/test.json or '
                    '/path/to/jsons/*',
                    ['raw_log_file'],
                    ValueError
                ))
        self._is_validated = True
        return self


class TrainingConfig(Config):
    """
    Classifier
    Scaler                  (optional)
    Data Parameters:
        - training days
        - from - to date
        - other filters, like hosts
    Model Parameters:  (optional)
        -
    """
    model_parameters = dict

    def __init__(self, config, parent=None):
        super(TrainingConfig, self).__init__(config, parent)
        self.allowed_models = [e.value for e in ModelEnum]

    def validate(self):
        logger.debug('Validating TrainingConfig...')
        if self.model:
            if self.model not in self.allowed_models:
                raise ValueError(
                    f'{self.model} is not in allowed models: '
                    f'{",".join(self.allowed_models)}'
                )

        if self.data_parameters:
            if not self.data_parameters.get('training_days') and not \
                    (self.data_parameters.get('from_date') and
                     self.data_parameters.get('to_date')):
                raise ValueError(
                    'Either training days or from-to date should be specified'
                )

        if not self.model_parameters:
            self.model_parameters = {}

        self._is_validated = True
        return self


class ElasticConfig(Config):
    """
    Configuration for ElasticSearch access.
    """
    user = ''
    password = ''
    host = ''
    base_index = ''
    index_type = ''

    def __init__(self, config):
        super(ElasticConfig, self).__init__(config)

    def validate(self):
        logger.debug('Validating ElasticConfig...')
        if not self.host:
            warnings.warn('Elastic search host is empty. If you are not using'
                          ' the Elasticsearch pipeline, ignore')

        self._is_validated = True
        return self


class MispConfig(Config):
    """
    Configuration for access to a MISP instance
    """
    misp_url = ''
    misp_key = ''
    misp_verifycert = True

    def __init__(self, config):
        super(MispConfig, self).__init__(config)

    def validate(self):
        logger.debug('Validating MispConfig...')
        if not self.misp_url:
            warnings.warn('Misp url is empty. If you are not using'
                          ' the MISP database, ignore')
        if not self.misp_key:
            warnings.warn('Misp key is empty. If you are not using'
                          ' the MISP database, ignore')

        self._is_validated = True
        return self


class MaintenanceConfig(Config):
    """
    Configuration for the database maintenance processes, like data
    partitioning and data archiving. Optional
    """
    template_folder = f'{get_default_data_path()}/templates'
    partition_table = 'request_sets'
    partition_field = 'created_at'
    partition_by = 'week'
    data_partition = None
    data_archive = None
    keep_data_for = '1 year'

    def __init__(self, config, parent=None):
        super().__init__(config, parent)
        if self.data_partition:
            self.data_partition = DataPartitionConfig(
                self.data_partition
            )
        if self.data_archive:
            self.data_archive = DataArchiveConfig(
                self.data_archive
            )

    def validate(self):
        logger.debug('Validating MaintenanceConfig...')
        if not self.template_folder:
            self.template_folder = f'{get_default_data_path()}/templates'
        if self.keep_data_for:
            split_kdf = self.keep_data_for.split(' ')
            if not len(split_kdf) == 2:
                raise ValueError(
                    f'Wrong value for \'keep_data_for\':{self.keep_data_for}'
                )
            unit = split_kdf[1]

            if unit not in ['year', 'years', 'month', 'months', 'week',
                            'weeks']:
                raise ValueError(
                    f'Wrong unit for \'keep_data_for\':{unit}'
                )

            if 'year' in unit:
                pass

        if self.partition_by not in ['week', 'month']:
            raise ValueError(
                f'{self.__class__.__name__}: invalid option '
                f'for partition_by {self.partition_by}'
            )
        if self.data_partition:
            self.data_partition = self.data_partition.validate()
        if self.data_archive:
            self.data_archive = self.data_archive.validate()

        if (self.data_partition or self.data_archive) and \
                not os.path.isdir(self.template_folder):
            self.add_error(ConfigError(
                f'Template folder not properly set: {self.template_folder}',
                ['template_folder'],
            ))
        self._is_validated = True
        return self


class DataPartitionConfig(Config):
    """
    Configuration for the database partitioning
    """
    since = None
    until = None
    index_by = ['target', 'ip']

    template = 'data_partitioning.jinja2'

    def validate(self):
        logger.debug('Validating DataPartitionConfig...')
        if isinstance(self.since, str):
            self.since = dateutil.parser.parse(self.since).replace(
                tzinfo=tzutc())
        if isinstance(self.until, str):
            self.until = dateutil.parser.parse(self.until).replace(
                tzinfo=tzutc())

        self._is_validated = True
        return self


class DataArchiveConfig(Config):
    """
    Configuration for the data archive process.
    """
    since = None
    until = None
    template = 'data_archive.jinja2'

    def validate(self):
        logger.debug('Validating DataArchiveConfig...')
        if isinstance(self.since, str):
            self.since = dateutil.parser.parse(self.since).replace(
                tzinfo=tzutc())
        if isinstance(self.until, str):
            self.until = dateutil.parser.parse(self.until).replace(
                tzinfo=tzutc())
        self._is_validated = True
        return self


class DatabaseConfig(Config):
    """
    Baskerville's database configuration - Mandatory.
    """
    name = None
    user = None
    password = None
    host = ''
    port = None
    type = 'postgres'
    maintenance = None

    def __init__(self, config, parent=None):
        super(DatabaseConfig, self).__init__(config, parent)
        if self.maintenance:
            self.maintenance = MaintenanceConfig(
                self.maintenance
            )

    def validate(self):
        logger.debug('Validating DatabaseConfig...')
        if not self.host:
            self.add_error(ConfigError(
                'Database host is empty.',
                ['host'],
            ))
            # raise ValueError('Database host is empty.')
        if not self.name:
            self.add_error(ConfigError(
                'Database name is empty.',
                ['name'],
            ))
        if not self.port:
            warnings.warn('Database port is empty. '
                          'The driver will be using the default value')
        if self.type not in ('mysql', 'postgres'):
            self.add_error(ConfigError(
                'Not implemented database connection for '
                '{} type'.format(self.type),
                ['type'],
                exception_type=NotImplementedError
            ))

        if self.maintenance:
            self.maintenance = self.maintenance.validate()

        self._is_validated = True
        return self


class KafkaConfig(Config):
    """
    Configuration for access to a Kafka instance for the kafka pipeline.
    """
    bootstrap_servers = '0.0.0.0:9092'
    zookeeper = 'localhost:2181'
    logs_topic = 'deflect.logs'
    features_topic = 'features'
    predictions_topic = 'predictions'
    banjax_command_topic = 'banjax_command_topic'
    banjax_report_topic = 'banjax_report_topic'
    security_protocol = 'PLAINTEXT'
    ssl_truststore_location = ''
    ssl_truststore_password = ''
    ssl_keystore_location = ''
    ssl_keystore_password = ''
    ssl_key_password = ''
    ssl_endpoint_identification_algorithm = ''
    ssl_check_hostname = False
    ssl_cafile = ''
    ssl_certfile = ''
    ssl_keyfile = ''

    def __init__(self, config):
        super(KafkaConfig, self).__init__(config)

    def validate(self):
        logger.debug('Validating KafkaConfig...')
        if not self.bootstrap_servers:
            self.add_error(ConfigError(
                'Kafka bootstrap_servers is empty.',
                ['bootstrap_servers'],
            ))
            # raise ValueError('Kafka bootstrap_servers is empty.')
        if not self.zookeeper:
            # kafka client can be used without zookeeper
            warnings.warn('Zookeeper url is empty.')
        if not self.logs_topic:
            warnings.warn('Logs topic is empty.')
        if not self.features_topic:
            warnings.warn('Features topic is empty')
        if not self.predictions_topic:
            warnings.warn('Predictions topic is empty.')

        self._is_validated = True
        return self


class SparkConfig(Config):
    """
    Spark related configuration.
    """
    app_name = None
    master = None
    parallelism = None
    log_conf = 'true'
    log_level = None
    session_timezone = None
    shuffle_partitions = None
    spark_driver_memory = None
    db_driver = None
    jars = ''
    metrics_conf = None
    jar_packages = None
    jars_repositories = None
    event_log = None
    serializer = None
    kryoserializer_buffer_max = None
    kryoserializer_buffer = None
    driver_java_options = None
    executor_extra_java_options = None
    driver_extra_class_path = None
    spark_executor_instances = None
    spark_executor_cores = None
    spark_executor_memory = None
    spark_python_profile = False
    storage_level = None
    off_heap_size = None
    redis_host = 'localhost'
    redis_port = 6379
    auth_secret = None
    ssl_enabled = False
    ssl_truststore = None
    ssl_truststore_password = None
    ssl_keystore = None
    ssl_keystore_password = None
    ssl_keypassword = None
    ssl_ui_enabled = False
    ssl_standalone_enabled = False
    ssl_history_server_enabled = False

    def __init__(self, config):
        super(SparkConfig, self).__init__(config)

    def validate(self):
        logger.debug('Validating SparkConfig...')
        from baskerville.spark.helpers import StorageLevelFactory

        if not self.storage_level:
            self.storage_level = StorageLevelFactory.get_storage_level(
                'OFF_HEAP'
            )
            logger.warn('Default spark storage level set to OFF_HEAP')
        else:
            self.storage_level = StorageLevelFactory.get_storage_level(
                self.storage_level
            )
        if self.db_driver is None:
            self.db_driver = 'org.postgresql.Driver'
            logger.info(f'Empty db_driver is set to {self.db_driver}')
        if not self.session_timezone:
            self.session_timezone = 'UTC'
            logger.info('session_timezone not defined, set default: UTC')
        if not self.master:
            if self.parallelism and self.parallelism > 0:
                self.master = 'local[{}]'.format(self.parallelism)
            else:
                self.master = 'local'
            warnings.warn('Master is empty, defalting to {}'.format(
                self.master
            ))
        if isinstance(self.shuffle_partitions, str):
            try:
                self.shuffle_partitions = int(self.shuffle_partitions)
            except ValueError:
                self.add_error(ConfigError(
                    'Spark shuffle_partitions should be an integer',
                    ['shuffle_partitions'],
                ))

        elif self.shuffle_partitions is None:
            self.shuffle_partitions = os.cpu_count() * 2
            warnings.warn(f'Spark shuffle partitions are set to'
                          f' {self.shuffle_partitions} (cpu count * 2).')
        if not self.db_driver:
            self.add_error(ConfigError(
                'Db driver cannot be empty',
                ['db_driver'],
            ))
            # raise ValueError('Db driver cannot be empty')
        if not self.event_log:
            self.event_log = 'false'
        else:
            self.event_log = 'true'

        if self.metrics_conf and not self.jar_packages:
            warnings.warn('Spark metrics configuration has been set but '
                          'jar packages is empty, '
                          'is this the correct behavior?')

        if not self.jars:
            warnings.warn('Spark jars is not defined, at least one jar for the'
                          'database connection should be defined, '
                          'please check.')

        if self.kryoserializer_buffer_max:
            if not isinstance(self.kryoserializer_buffer_max, str):
                self.add_error(ConfigError(
                    'Wrong value for serializer_buffer_max, '
                    'please use string values, e.g.: \'2048m\' '
                    '(maximum value)',
                    ['kryoserializer_buffer_max'],
                ))
            if self.kryoserializer_buffer_max[-1] == 'm' and \
                    int(self.kryoserializer_buffer_max[:-1]) > 2048:
                self.add_error(ConfigError(
                    'Spark serializer_buffer_max cannot be more than 2048m',
                    ['kryoserializer_buffer_max'],
                ))
                # raise ValueError(
                #     'Spark serializer_buffer_max cannot be more than 2048m')

        if self.kryoserializer_buffer:
            if not isinstance(self.kryoserializer_buffer, str):
                self.add_error(ConfigError(
                    'Wrong value for serializer_buffer, '
                    'please use string values, e.g.: \'2048k\'',
                    ['kryoserializer_buffer'],
                ))
                # raise ValueError('Wrong value for serializer_buffer, '
                #                  'please use string values, e.g.: \'2048k\'')
            if self.kryoserializer_buffer[-1] == 'k' and \
                    int(self.kryoserializer_buffer[:-1]) > 2048:
                self.add_error(ConfigError(
                    'Spark serializer_buffer_max cannot be more than 2048k',
                    ['serializer_buffer_max'],
                ))
                # raise ValueError(
                #     'Spark serializer_buffer_max cannot be more than 2048k')
            if self.kryoserializer_buffer[-1] == \
                    self.kryoserializer_buffer_max[-1]:
                if int(self.kryoserializer_buffer_max[:-1]) < \
                        int(self.kryoserializer_buffer[:-1]):
                    self.add_error(ConfigError(
                        'serializer_buffer\'s value cannot be '
                        'greater than serializer_buffer_max',
                        ['kryoserializer_buffer'],
                    ))
                    # raise ValueError('serializer_buffer\'s value cannot be '
                    #                  'greater than serializer_buffer_max')

        if self.ssl_enabled:
            if not self.ssl_truststore:
                self.add_error(ConfigError('ssl_truststore must be specified if ssl_enabled is true'))
            if not self.ssl_keystore:
                self.add_error(ConfigError('ssl_keystore must be specified if ssl_enabled is true'))

        self._is_validated = True
        return self


class MetricsConfig(Config):
    port = 8998
    performance = None
    progress = False

    def __init__(self, config_dict):
        super(MetricsConfig, self).__init__(config_dict)

    def validate(self):
        logger.debug('Validating MetricsConfig...')
        if not self.performance and not self.progress:
            raise ValueError('Metrics configuration is missing.')

        if not self.port:
            self.port = 8998
            warnings.warn(f'Metrics port is set to default {self.port}')
        if not isinstance(self.port, int):
            self.port = int(self.port)

        self._is_validated = True
        return self


class DataParsingConfig(Config):
    """
    The configuration for the incoming data parsing
    """
    schema = None
    schema_obj = None
    parser = 'JSONLogSparkParser'
    group_by_cols = ('client_request_host', 'client_ip')
    timestamp_column = '@timestamp'

    def __init__(self, config_dict):
        super().__init__(config_dict)

    def validate(self):
        logger.debug('Validating DataParsingConfig...')
        from baskerville.models.log_parsers import LOG_PARSERS

        if not self.schema or not os.path.isfile(self.schema):
            self.add_error(ConfigError(
                'Please provide a valid schema', 'schema'))
        else:
            with open(self.schema) as sc:
                self.schema_obj = json.load(sc)
        if not self.parser:
            self.parser = LOG_PARSERS.values()[0]
            warnings.warn(f'Log parser set to default: {self.parser.__name__}')
        if self.parser not in LOG_PARSERS:
            self.add_error(ConfigError(
                f'Log parser {self.parser} has not been implemented.',
                ['parser'],
                exception_type=NotImplementedError
            ))

        if self.schema_obj:
            self.parser = LOG_PARSERS[self.parser](
                self.schema_obj, drop_row_if_missing=self.group_by_cols
            )

        if not self.timestamp_column:
            raise ValueError('A timestamp_column must be provided')

        self._is_validated = True
        return self
