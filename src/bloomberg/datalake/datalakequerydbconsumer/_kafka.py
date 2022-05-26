"""
 ** Copyright 2021 Bloomberg Finance L.P.
 **
 ** Licensed under the Apache License, Version 2.0 (the "License");
 ** you may not use this file except in compliance with the License.
 ** You may obtain a copy of the License at
 **
 **     http://www.apache.org/licenses/LICENSE-2.0
 **
 ** Unless required by applicable law or agreed to in writing, software
 ** distributed under the License is distributed on an "AS IS" BASIS,
 ** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 ** See the License for the specific language governing permissions and
 ** limitations under the License.
"""


from __future__ import annotations

import json
import logging
import os
from threading import Event
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaError, KafkaException

from ._db_accessor import (
    _add_client_tags,
    _add_column_metrics,
    _add_failed_event,
    _add_operator_summaries,
    _add_output_column_sources,
    _add_output_columns,
    _add_query_metrics,
    _add_resource_groups,
)

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
KAFKA_TOPIC = os.getenv("DATALAKEQUERYDBCONSUMER_KAFKA_TOPIC")
KAFKA_GROUP_ID = os.getenv("DATALAKEQUERYDBCONSUMER_KAFKA_GROUP_ID")


class KafkaConsumer:
    def __init__(self, stop_event: Event | None = None, post_message_hook: Callable[[Any], None] | None = None) -> None:
        _config = self._get_config()
        self._consumer = Consumer(_config)
        logging.debug("Using Kafka Consumer configuration\n%s", self._get_config())

        self._consumer.subscribe([KAFKA_TOPIC])
        logging.debug("Subscribing Kafka Consumer to topic %s", KAFKA_TOPIC)

        self._post_message_hook = post_message_hook

        if stop_event is None:
            self._stop_event = Event()
        else:
            self._stop_event = stop_event

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                message = self._consumer.poll(timeout=1.0)

                # message processing block starts
                if message is None:
                    continue
                elif message.error():
                    if message.error().fatal():
                        logging.error(f"ABORTING: Fatal confluent_kafka error caught on consume: {message.error()}")
                        raise KafkaException(message.error())
                    elif message.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        logging.error(f"Permanent consumer error: {message.error()}")
                    else:
                        logging.warn(f"Non-critical consume event: {message.error()}")
                else:
                    self._handle_message(message)

                    if self._post_message_hook is not None:
                        self._post_message_hook(message)

            except Exception:
                logging.exception("Caught exception while listening for messages")

    def stop(self) -> None:
        self._stop_event.set()

    def _get_config(self) -> Any:
        return {
            "metadata.broker.list": KAFKA_BROKERS,
            "client.id": "datalakequerydbconsumer",
            "enable.auto.offset.store": True,
            "log.connection.close": False,
            "enable.partition.eof": False,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "smallest",
        }

    def _handle_message(self, message: Any) -> None:
        """
        Handles an incoming message with 3 cases:
        1. Message is proccessed successfully - Great! Commit and move on
        2. Revocable error is raised - If current retry < max retries
            add the same message to the queue, incrementing the retry count.
            Else it becomes non-revocable
        3. Non-revocable error - Add retry information to failed table
        """
        logging.debug(
            "Received Message"
            + "[ length = %s bytes, topic = %s, partition = %s, key = %s, offset = %s, timestamp = %s UTC ]",
            len(message),
            message.topic(),
            message.partition(),
            message.key(),
            message.offset(),
            message.timestamp()[1],
        )

        try:
            _raw_metrics = json.loads(message.value())
            logging.debug("Loaded message from string")
            _add_query_metrics(_raw_metrics)
            logging.debug("Saved message query metrics")
            _add_column_metrics(_raw_metrics)
            logging.debug("Saved message column metrics")
            _add_client_tags(_raw_metrics)
            logging.debug("Saved message client tags")
            _add_resource_groups(_raw_metrics)
            logging.debug("Saved message resource groups")
            _add_operator_summaries(_raw_metrics)
            logging.debug("Saved message operator summaries")
            _add_output_columns(_raw_metrics)
            logging.debug("Saved message output columns")
            _add_output_column_sources(_raw_metrics)
            logging.debug("Saved message output columns sources")
        except Exception:
            logging.exception("Caught exception while processing metrics")
            _add_failed_event(message.value())
