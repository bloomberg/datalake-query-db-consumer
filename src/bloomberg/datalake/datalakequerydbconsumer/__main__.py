#!/opt/bb/bin/python3.9
import logging
import logging.config
from pathlib import Path

import yaml

from ._kafka import KafkaConsumer

with open(Path("/") / "opt" / "config" / "datalakequerydbconsumer" / "log_config.yaml", "r") as handle:
    logging.config.dictConfig(yaml.safe_load(handle))


def run() -> None:
    logging.info("Starting consumer")
    KafkaConsumer().run()
    logging.info("Kafka consumer stopped")

    logging.info("Exiting")


if __name__ == "__main__":
    run()
