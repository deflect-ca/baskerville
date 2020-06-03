#!/usr/bin/env python
import argparse
import atexit
import os
import time

from datetime import timedelta

from prometheus_client import start_http_server

from baskerville import src_dir
from baskerville.models.engine import BaskervilleAnalyticsEngine
from baskerville.simulation.real_timeish_simulation import simulation
from baskerville.util.helpers import get_logger, parse_config, \
    get_default_data_path

# Main baskerville script

PROCESS_LIST = []
baskerville_engine = None
logger = None


os.environ['TZ'] = 'UTC'

try:
    time.tzset()
except AttributeError:
    print('Cannot load time.tzset on Windows')


def run_simulation(conf, spark=None):
    """
    Creates a separate process to run the simulation script that publishes into
    kafka consume_topic (where baskerville should listen to consume from)
    :param BaskervilleConfig conf: current Baskerville configuration
    :param SparkSession spark:
    :return: None
    """
    from multiprocessing import Process

    kafka_conf = conf.kafka
    engine_conf = conf.engine

    simulation_process = Process(
        name='SimulationThread',
        target=simulation,
        args=[
            engine_conf.simulation.log_file,
            timedelta(seconds=engine_conf.time_bucket),
        ],
        kwargs={
            'topic_name': kafka_conf['consume_topic'],
            'sleep': engine_conf.simulation.sleep,
            'kafka_url': kafka_conf.bootstrap_servers,
            'zookeeper_url': kafka_conf.zookeeper,
            'verbose': engine_conf.simulation.verbose,
            'spark': spark,
        }
    )
    PROCESS_LIST.append(simulation_process)
    print('Set up Simulation...')


def populate_with_test_data(database_config):
    """
    Load the test data and save them in the database
    :param dict[str, T] database_config:
    :return:
    """
    global logger
    from baskerville.util.model_serialization import import_pickled_model
    path = os.path.join(get_default_data_path(), 'samples', 'sample_model')
    test_model_path = os.path.join(
        get_default_data_path(), 'samples', 'test_model')
    logger.info(f'Loading test model from: {test_model_path}')
    import_pickled_model(database_config, path, test_model_path)


def main():
    """
    Baskerville commandline arguments
    :return:
    """
    global baskerville_engine, logger
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "pipeline",
        help="Pipeline to use: es, rawlog, or kafka",
    )
    parser.add_argument(
        "-s", "--simulate", dest="simulate", action="store_true",
        help="Simulate real-time run using kafka",
    )
    parser.add_argument(
        "-e", "--startexporter", dest="start_exporter",
        action="store_true",
        help="Start the Baskerville Prometheus exporter at the specified "
             "in the configuration port",
    )

    parser.add_argument(
        "-t", "--testmodel", dest="test_model",
        help="Add a test model in the models table",
        default=False,
        action="store_true"
    )

    parser.add_argument(
        "-c", "--conf", action="store", dest="conf_file",
        default=os.path.join(src_dir, '..', 'conf', 'baskerville.yaml'),
        help="Path to config file"
    )

    args = parser.parse_args()
    conf = parse_config(path=args.conf_file)

    baskerville_engine = BaskervilleAnalyticsEngine(
        args.pipeline, conf, register_metrics=args.start_exporter
    )
    logger = get_logger(
        __name__,
        logging_level=baskerville_engine.config.engine.log_level,
        output_file=baskerville_engine.config.engine.logpath
    )

    # start simulation if specified
    if args.simulate:
        spark = None
        if baskerville_engine.config.engine.use_spark:
            from baskerville.spark import get_spark_session
            spark = get_spark_session()  # baskerville.pipeline.spark

        logger.info('Starting simulation...')
        run_simulation(baskerville_engine.config, spark)

    # start baskerville prometheus exporter if specified
    if args.start_exporter:
        if not baskerville_engine.config.engine.metrics:
            raise RuntimeError('Cannot start exporter without metrics config')
        port = baskerville_engine.config.engine.metrics.port
        start_http_server(port)
        logger.info(f'Starting Baskerville Exporter at '
                    f'http://localhost:{port}')

    # populate with test data if specified
    if args.test_model:
        populate_with_test_data(conf['database'])

    for p in PROCESS_LIST[::-1]:
        print(f"{p.name} starting...")
        p.start()

    logger.info('Starting Baskerville Engine...')
    baskerville_engine.run()


if __name__ == "__main__":

    @atexit.register
    def clean_up_before_shutdown():
        global baskerville_engine, logger

        if not logger:
            logger = get_logger('clean_up_before_shutdown')

        logger.info('Just a sec, finishing up...')
        if baskerville_engine:
            logger.info('Finishing up Baskerville...')
            baskerville_engine.finish_up()
        for each in PROCESS_LIST:
            each.terminate()
            each.join()
            logger.info(f'Stopped {each.name}...')

    main()
