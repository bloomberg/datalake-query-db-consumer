from __future__ import annotations

import logging
import os
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Producer

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
        logging.debug("Created producer")

    def enqueue_message(self, message: bytes, key: bytes) -> None:
        try:
            logging.debug("Producing message: %s", message)
            self._producer.produce(self._topic, message, key=key, callback=self._delivery_callback, partition=0)
        except KafkaException as e:
            if e.args[0].fatal():
                logging.fatal(
                    f"ABORTING: Fatal confluent_kafka error caught on produce: {e}",
                    exc_info=True,
                )
                raise
            else:
                logging.error(f"Produce failed: {e}", exc_info=True)

        not_flushed = self._producer.flush(10)
        if not_flushed > 0:
            raise KafkaError(f"KafkaProducer timed out with {not_flushed} messages not flushed")

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        if err:
            logging.error(
                "Message delivery successful for Message"
                + "[ length = %s bytes, topic = %s, partition = %s, key = %s ]: %s",
                len(msg),
                msg.topic(),
                msg.partition(),
                msg.key(),
                err,
            )
        else:
            logging.debug(
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
        logging.info("Closing producer")
