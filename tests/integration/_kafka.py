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

import logging
import os
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Producer

logger = logging.getLogger(__name__)

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
KAFKA_TOPIC = os.getenv("DATALAKEQUERYDBCONSUMER_KAFKA_TOPIC")


class KafkaProducer:
    def __init__(self) -> None:
        self._topic = KAFKA_TOPIC

        conf = {
            "metadata.broker.list": KAFKA_BROKERS,
            "client.id": self._topic,
            "log.connection.close": False,
            "queue.buffering.max.ms": 100,
            "enable.idempotence": True,
            "message.timeout.ms": 600000,
            "compression.type": "snappy",
        }

        self._producer = Producer(conf)
        logger.debug("Created producer")

    def enqueue_message(self, message: bytes, key: bytes) -> None:
        try:
            logger.debug("Producing message: %s", message)
            self._producer.produce(self._topic, message, key=key, callback=self._delivery_callback, partition=0)
        except KafkaException as e:
            if e.args[0].fatal():
                logger.fatal(
                    "ABORTING: Fatal confluent_kafka error caught on produce: %s",
                    e,
                    exc_info=True,
                )
                raise
            else:
                logger.error("Produce failed: %s", e, exc_info=True)

        not_flushed = self._producer.flush(10)
        if not_flushed > 0:
            raise KafkaError("KafkaProducer timed out with %s messages not flushed", not_flushed)

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        if err:
            logger.error(
                "Message delivery successful for Message"
                + "[ length = %s bytes, topic = %s, partition = %s, key = %s ]: %s",
                len(msg),
                msg.topic(),
                msg.partition(),
                msg.key(),
                err,
            )
        else:
            logger.debug(
                "Message delivery successful for Message"
                + "[ length = %s bytes, topic = %s, partition = %s, key = %s, offset = %s, timestamp = %s UTC ]",
                len(msg),
                msg.topic(),
                msg.partition(),
                msg.key(),
                msg.offset(),
                msg.timestamp()[1],
            )

    def close(self) -> None:
        self._producer = None  # type: ignore
        logger.info("Closing producer")
