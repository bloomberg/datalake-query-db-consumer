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
