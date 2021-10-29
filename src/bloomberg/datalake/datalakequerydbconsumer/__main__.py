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
# !/usr/local/bin/python3.9


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
