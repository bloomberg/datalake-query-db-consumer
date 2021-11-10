from __future__ import annotations

import json
import logging
import os
from json.decoder import JSONDecodeError
from threading import Event
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaError, KafkaException
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from ._db_accessor import _add_column_metrics, _add_query_metrics

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

                    self._consumer.store_offsets(message=message)

            except Exception as e:
                logging.exception(e)

    def stop(self) -> None:
        self._stop_event.set()

    def _get_config(self) -> Any:
        return {
            "metadata.broker.list": KAFKA_BROKERS,
            "client.id": "datalakequerydbconsumer",
            "enable.auto.offset.store": False,
            "log.connection.close": False,
            "enable.partition.eof": False,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "smallest",
        }

    def _handle_message(self, message: Any) -> None:
        """
        Handles an incoming message.
        If this function return normally it is considered that the message has been
        processed successfully and will be commited.
        If this function raises any exception the message is considered unprocessed and
        offsets will not be commited.
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
            _add_query_metrics(_raw_metrics)
            _add_column_metrics(_raw_metrics)
        except (ValueError, TypeError, JSONDecodeError) as ex:
            # Can't fix a badly formated message
            logging.exception(ex)
        except OperationalError as ex:
            # Operational errors are errors related to the database itself,
            # and can be caused by timeouts or temporary unavailability.
            # Its worth retrying on these.
            raise ex
        except SQLAlchemyError as ex:
            # These are usually problems related to grammar or insertions, not fixable
            logging.exception(ex)
