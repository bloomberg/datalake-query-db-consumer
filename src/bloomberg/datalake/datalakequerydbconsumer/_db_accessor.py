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
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ._data_models import get_column_metrics_from_raw, get_query_metrics_from_raw

POSTGRES_DB_URL = os.environ.get("DATALAKEQUERYDBCONSUMER_DB_URL")

engine = create_engine(POSTGRES_DB_URL)
Session = sessionmaker(bind=engine)


def _add_query_metrics(raw_metrics: dict[str, Any]) -> None:
    query_metrics = get_query_metrics_from_raw(raw_metrics)
    with Session() as session:
        session.add(query_metrics)
        session.commit()


def _add_column_metrics(raw_metrics: dict[str, Any]) -> None:
    column_metrics = get_column_metrics_from_raw(raw_metrics)
    with Session() as session:
        session.add_all(column_metrics)
        session.commit()
