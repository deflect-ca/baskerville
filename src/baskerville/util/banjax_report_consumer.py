# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import json
from kafka import KafkaConsumer
from baskerville.models.ip_cache import IPCache


class BanjaxReportConsumer(object):

    def __init__(self, config, logger):
        self.config = config
        self.kafka_config = config.kafka
        self.logger = logger
        self.ip_cache = IPCache(config, self.logger)

    def run(self):
        consumer = KafkaConsumer(
            self.kafka_config.banjax_report_topic,
            group_id="baskerville_postprocessing",
            auto_offset_reset='earliest',
            **self.config.kafka.connection
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

            if d.get("name") == "ip_failed_challenge":
                self.consume_ip_failed_challenge_message(d)
            elif d.get("name") == "ip_passed_challenge":
                self.consume_ip_passed_challenge_message(d)
            elif d.get("name") == "ip_banned":
                self.consume_ip_banned_message(d)

    def consume_ip_failed_challenge_message(self, message):
        self.ip_cache.ip_failed_challenge(message['value_ip'])

    def consume_ip_passed_challenge_message(self, message):
        self.ip_cache.ip_passed_challenge(message['value_ip'])

    def consume_ip_banned_message(self, message):
        pass
