import datetime
import threading
import json
from kafka import KafkaConsumer, KafkaProducer
import time
import logging
import sys
import types

from baskerville.db import set_up_db
from baskerville.models.config import KafkaConfig
from baskerville.models.ip_cache import IPCache
from baskerville.util.helpers import parse_config
import argparse
import os
from baskerville import src_dir


class BanjaxReportConsumer(object):
    status_message_fields = [
        "timestamp",
        "restart_time",
        "reload_time",
        "num_of_challenges",
        "num_of_host_challenges",
        "num_of_ip_challenges",
        "swabber_ip_db_size",
        "regex_manager_ip_db_size",
        "challenger_ip_db_size",
        "proxy.process.traffic_server.memory.rss",
        "proxy.node.cache.contents.num_docs",
        "proxy.process.cache.bytes_total",
        "proxy.process.cache.percent_full",
        "proxy.process.cache.ram_cache.bytes_used",
        "proxy.process.cache.ram_cache.total_bytes",
        "proxy.process.net.connections_currently_open",
        "proxy.process.current_server_connections",
        "proxy.process.http.current_active_client_connections",
        "proxy.process.eventloop.time.max"
    ]

    def __init__(self, config, logger):
        self.config = config
        self.kafka_config = config.kafka
        self.logger = logger
        self.ip_cache = IPCache(config, self.logger)
        self.session, self.engine = set_up_db(config.database.__dict__)

        # XXX i think the metrics registry swizzling code is passing
        # an extra argument here mistakenly?.?.
        def _tmp_fun(_, _2, message):
            return message

        for field_name in self.__class__.status_message_fields:
            setattr(self, f"consume_{field_name}", types.MethodType(_tmp_fun, self))

    def run(self):
        consumer = KafkaConsumer(
            self.kafka_config.banjax_report_topic,
            group_id=None,
            bootstrap_servers=self.kafka_config.bootstrap_servers,
            security_protocol=self.kafka_config.security_protocol,
            ssl_check_hostname=self.kafka_config.ssl_check_hostname,
            ssl_cafile=self.kafka_config.ssl_cafile,
            ssl_certfile=self.kafka_config.ssl_certfile,
            ssl_keyfile=self.kafka_config.ssl_keyfile,
        )

        for message in consumer:
            self.consume_message(message)

        consumer.close()

    def consume_message(self, message):
        if len(message.value) > 0:
            try:
                s = message.value.decode("utf-8")
            except UnicodeDecodeError:
                self.logger.info("got bad utf-8 over the kafka channel")

            try:
                d = json.loads(s)
            except json.JSONDecodeError:
                self.logger.info(f"got bad json over the kafka channel: {s}")

            # 'status'-type messages contain several metrics and are reported per $interval
            if d.get("name") == "status":
                edge_id = d.get("id")
                for k, _ in d.items():
                    if k == 'name' or k == 'id':
                        continue
                    try:
                        f = getattr(self, f"consume_{k}")
                        f(self, d)
                    except AttributeError:
                        self.logger.info(f"did not process banjax status {k} from edge {edge_id}")

            # 'ip_failed_challenge'-type messages are reported when a challenge is failed
            elif d.get("name") == "ip_failed_challenge":
                self.consume_ip_failed_challenge_message(d)
            elif d.get("name") == "ip_passed_challenge":
                self.consume_ip_passed_challenge_message(d)
            elif d.get("name") == "ip_banned":
                self.consume_ip_banned_message(d)

    def get_time_filter(self):
        return (datetime.datetime.utcnow() - datetime.timedelta(
            minutes=self.config.engine.banjax_sql_update_filter_minutes)).strftime("%Y-%m-%d %H:%M:%S %z")

    def consume_ip_failed_challenge_message(self, message):
        # ip = message['value_ip']
        # num_fails = self.ip_cache.ip_failed_challenge(ip)
        # if num_fails > 0:
        #     try:
        #         sql = f'update request_sets set challenge_failed = {num_fails} where ' \
        #               f'stop > \'{self.get_time_filter()}\' and ip = \'{ip}\''
        #         self.session.execute(sql)
        #         self.session.commit()
        #
        #     except Exception:
        #         self.session.rollback()
        #         self.logger.error(Exception)
        #         raise

        return message

    def consume_ip_passed_challenge_message(self, message):
        # ip = message['value_ip']
        # self.logger.info(f'Banjax ip_passed_challenge {ip} ...')
        # try:
        #     sql = f'update request_sets set challenge_passed = 1 where ' \
        #           f'stop > \'{self.get_time_filter()}\' and ip = \'{ip}\''
        #     self.session.execute(sql)
        #     self.session.commit()
        #
        # except Exception:
        #     self.session.rollback()
        #     self.logger.error(Exception)
        #     raise

        return message

    def consume_ip_banned_message(self, message):
        ip = message['value_ip']
        self.logger.info(f'Banjax ip_banned {ip} ...')
        # try:
        #     sql = f'update request_sets set banned = 1 where ' \
        #           f'stop > \'{self.get_time_filter()}\' and ip = \'{ip}\''
        #     self.session.execute(sql)
        #     self.session.commit()
        #
        # except Exception:
        #     self.session.rollback()
        #     self.logger.error(Exception)
        #     raise

        return message


class ChallengeProducer(object):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def run(self):
        producer = KafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
            security_protocol=self.config.security_protocol,
            ssl_check_hostname=self.config.ssl_check_hostname,
            ssl_cafile=self.config.ssl_cafile,
            ssl_certfile=self.config.ssl_certfile,
            ssl_keyfile=self.config.ssl_keyfile,
        )

        number = 0
        while True:
            for _ in range(0, 10):
                domain = f"example-{number}.com:8080"
                command = {'name': 'challenge_host', 'value': domain}
                producer.send(self.config.banjax_command_topic, json.dumps(command).encode('utf-8'))
                self.logger.info("sent a command")
                number = number + 1
            time.sleep(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--conf", action="store", dest="conf_file",
        default=os.path.join(src_dir, '..', 'conf', 'baskerville.yaml'),
        help="Path to config file"
    )

    parser.add_argument(
        "-c", "--consumer", dest="start_consumer", action="store_true",
        help="start consumer",
    )

    parser.add_argument(
        "-p", "--producer", dest="start_producer", action="store_true",
        help="start consumer",
    )

    args = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger = logging.getLogger()

    config_dict = KafkaConfig(parse_config(path=args.conf_file)['kafka']).validate()

    if args.start_consumer:
        status_consumer = BanjaxReportConsumer(config_dict, logger)
        consumer_thread = threading.Thread(target=status_consumer.run)
        consumer_thread.start()

    if args.start_producer:
        challenge_producer = ChallengeProducer(config_dict, logger)
        producer_thread = threading.Thread(target=challenge_producer.run)
        producer_thread.start()

    if args.start_consumer:
        consumer_thread.join()

    if args.start_producer:
        producer_thread.join()
