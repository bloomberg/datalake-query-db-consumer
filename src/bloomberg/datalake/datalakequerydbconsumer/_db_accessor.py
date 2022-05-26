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

import os
from typing import Any, Callable

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tenacity import retry, stop, wait

from ._data_models import (
    Base,
    FailedEvent,
    get_client_tags_from_raw,
    get_column_metrics_from_raw,
    get_operator_summaries_from_raw,
    get_query_metrics_from_raw,
    get_resource_groups_from_raw,
)

DB_URL = os.environ.get("DATALAKEQUERYDBCONSUMER_DB_URL")

engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)


@retry(stop=stop.stop_after_attempt(4), wait=wait.wait_exponential(multiplier=1, min=1, max=10))
def __commit_db_model(db_model: Base | list[Base]) -> None:
    with Session() as session:
        if isinstance(db_model, list):
            session.add_all(db_model)
        else:
            session.add(db_model)
        session.commit()


def __create_add_function(parser: Callable[[dict[str, Any]], Base | list[Base]]) -> Callable[[dict[str, Any]], None]:
    def __func(raw_metrics: dict[str, Any]) -> None:
        db_model = parser(raw_metrics)
        __commit_db_model(db_model)

    return __func


_add_query_metrics = __create_add_function(get_query_metrics_from_raw)

_add_column_metrics = __create_add_function(get_column_metrics_from_raw)

_add_client_tags = __create_add_function(get_client_tags_from_raw)

_add_resource_groups = __create_add_function(get_resource_groups_from_raw)

_add_operator_summaries = __create_add_function(get_operator_summaries_from_raw)


def _add_failed_event(event: Any) -> None:
    if isinstance(event, bytes):
        __commit_db_model(FailedEvent(event=event.decode("utf-8")))
    elif isinstance(event, str):
        __commit_db_model(FailedEvent(event=event))
    else:
        __commit_db_model(FailedEvent(event=str(event)))
