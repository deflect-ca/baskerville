import threading
import json
from kafka import KafkaConsumer, KafkaProducer
import time
import logging
import sys
import types
from baskerville.models.config import KafkaConfig
from baskerville.util.helpers import parse_config
import argparse
import os
from baskerville import src_dir


class StatusConsumer(object):
    status_message_fields = [
        "num_of_challenges",
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

    def __init__(self, config_dict, logger):
        self.config = config_dict
        self.logger = logger

        # XXX i think the metrics registry swizzling code is passing
        # an extra argument here mistakenly?.?.
        def _tmp_fun(_, _2, message):
            return message

        for field_name in self.__class__.status_message_fields:
            setattr(self, f"consume_{field_name}", types.MethodType(_tmp_fun, self))

    def run(self):
        consumer = KafkaConsumer(
            self.config.banjax_report_topic,
            bootstrap_servers=self.config.bootstrap_servers,
            security_protocol=self.config.security_protocol,
            ssl_check_hostname=self.config.ssl_check_hostname,
            ssl_cafile=self.config.ssl_cafile,
            ssl_certfile=self.config.ssl_certfile,
            ssl_keyfile=self.config.ssl_keyfile,
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

            self.logger.info(f"got a message: {d}")

            # 'status'-type messages contain several metrics and are reported per $interval
            if d.get("name") == "status":
                for k, _ in d.items():
                    try:
                        self.logger.info(f"try to dispatch on {k}")
                        f = getattr(self, f"consume_{k}")
                        f(self, d)
                        self.logger.info(f"dispatched {k}")
                    except AttributeError:
                        self.logger.info(f"did not dispatch {k}")

            # 'ip_failed_challenge'-type messages are reported when a challenge is failed
            elif d.get("name") == "ip_failed_challenge":
                self.consume_ip_failed_challenge_message(d)

    def consume_ip_failed_challenge_message(self, message):
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
                producer.send(self.config.get('banjax_command_topic'), json.dumps(command).encode('utf-8'))
                self.logger.info("sent a command")
                number = number + 1
            time.sleep(0.1)


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

    config_dict = KafkaConfig(parse_config(path=args.conf_file)).validate()

    if args.start_consumer:
        status_consumer = StatusConsumer(config_dict, logger)
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
