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

from datetime import datetime
from json import loads
from typing import Any, TypeVar, cast

from dateutil import parser
from sqlalchemy import JSON, Column, DateTime, Float, Integer, String, UnicodeText, func
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import BigInteger

Base = declarative_base()

T = TypeVar("T")


class QueryMetrics(Base):  # type: ignore
    __tablename__ = "query_metrics"

    queryId = Column("queryId", String(100), primary_key=True)
    transactionId = Column("transactionId", String(100))
    query = Column("query", String(10000))
    remoteClientAddress = Column("remoteClientAddress", String(100))
    user = Column("user", String(100))
    userAgent = Column("userAgent", String(100))
    source = Column("source", String(100))
    serverAddress = Column("serverAddress", String(100))
    serverVersion = Column("serverVersion", String(100))
    environment = Column("environment", String(10))
    queryType = Column("queryType", String(50))
    uri = Column("uri", String(255), nullable=True)
    plan = Column("plan", UnicodeText, nullable=True)
    payload = Column("paylod", JSON(none_as_null=True), nullable=True)
    sessionProperties = Column("sessionProperties", JSON(none_as_null=True), nullable=True)
    cpuTime = Column("cpuTime", Float)
    failedCpuTime = Column("failedCpuTime", Float, nullable=True)
    wallTime = Column("wallTime", Float)
    queuedTime = Column("queuedTime", Float)
    scheduledTime = Column("scheduledTime", Float)
    failedScheduledTime = Column("failedScheduledTime", Float, nullable=True)
    analysisTime = Column("analysisTime", Float)
    planningTime = Column("planningTime", Float)
    executionTime = Column("executionTime", Float)
    inputBlockedTime = Column("inputBlockedTime", Float, nullable=True)
    failedInputBlockedTime = Column("failedInputBlockedTime", Float, nullable=True)
    outputBlockedTime = Column("outputBlockedTime", Float, nullable=True)
    failedOutputBlockedTime = Column("failedOutputBlockedTime", Float, nullable=True)
    peakUserMemoryBytes = Column("peakUserMemoryBytes", BigInteger)
    peakTotalNonRevocableMemoryBytes = Column("peakTotalNonRevocableMemoryBytes", BigInteger, nullable=True)
    peakTaskUserMemory = Column("peakTaskUserMemory", BigInteger)
    peakTaskTotalMemory = Column("peakTaskTotalMemory", BigInteger)
    physicalInputBytes = Column("physicalInputBytes", BigInteger)
    physicalInputRows = Column("physicalInputRows", BigInteger)
    internalNetworkBytes = Column("internalNetworkBytes", BigInteger)
    internalNetworkRows = Column("internalNetworkRows", BigInteger)
    totalBytes = Column("totalBytes", BigInteger)
    totalRows = Column("totalRows", BigInteger)
    outputBytes = Column("outputBytes", BigInteger)
    outputRows = Column("outputRows", BigInteger)
    writtenBytes = Column("writtenBytes", BigInteger)
    writtenRows = Column("writtenRows", BigInteger)
    processedInputBytes = Column("processedInputBytes", BigInteger, nullable=True)
    processedInputRows = Column("processedInputRows", BigInteger, nullable=True)
    cumulativeMemory = Column("cumulativeMemory", Float)
    completedSplits = Column("completedSplits", Integer)
    resourceWaitingTime = Column("resourceWaitingTime", Float)
    planNodeStatsAndCosts = Column("planNodeStatsAndCosts", JSON(none_as_null=True), nullable=True)
    createTime = Column("createTime", DateTime)
    executionStartTime = Column("executionStartTime", DateTime)
    endTime = Column("endTime", DateTime)

    __table_args__ = {"schema": "raw_metrics", "extend_existing": True}


class ColumnMetrics(Base):  # type: ignore
    __tablename__ = "column_metrics"

    queryId = Column("queryId", String(100), ForeignKey(QueryMetrics.queryId), primary_key=True)
    catalogName = Column("catalogName", String(100), primary_key=True)
    schemaName = Column("schemaName", String(100), primary_key=True)
    tableName = Column("tableName", String(100), primary_key=True)
    columnName = Column("columnName", String(100), primary_key=True)
    physicalInputBytes = Column("physicalInputBytes", Integer)
    physicalInputRows = Column("physicalInputRows", Integer)

    __table_args__ = {"extend_existing": True, "schema": "raw_metrics"}

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ColumnMetrics):
            return False
        return cast(
            bool,
            self.queryId == other.queryId
            and self.catalogName == other.catalogName
            and self.schemaName == other.schemaName
            and self.tableName == other.tableName
            and self.columnName == other.columnName,
        )

    def __hash__(self) -> int:
        return hash(frozenset([self.queryId, self.catalogName, self.schemaName, self.tableName, self.columnName]))


class ClientTags(Base):  # type: ignore

    __tablename__ = "client_tags"

    queryId = Column("queryId", String(100), ForeignKey(QueryMetrics.queryId), primary_key=True)
    clientTag = Column("clientTag", String(255), primary_key=True)

    __table_args__ = {"extend_existing": True, "schema": "raw_metrics"}

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ColumnMetrics):
            return False
        return cast(bool, self.queryId == other.queryId and self.clientTag == other.clientTag)

    def __hash__(self) -> int:
        return hash(frozenset([self.queryId, self.clientTag]))


class ResourceGroups(Base):  # type: ignore

    __tablename__ = "resource_groups"

    queryId = Column("queryId", String(100), ForeignKey(QueryMetrics.queryId), primary_key=True)
    resourceGroup = Column("resourceGroup", String(255), primary_key=True)

    __table_args__ = {"extend_existing": True, "schema": "raw_metrics"}

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ColumnMetrics):
            return False
        return cast(bool, self.queryId == other.queryId and self.resourceGroup == other.resourceGroup)

    def __hash__(self) -> int:
        return hash(frozenset([self.queryId, self.resourceGroup]))


class OperatorSummaries(Base):  # type: ignore

    __tablename__ = "operator_summaries"

    queryId = Column("queryId", String(100), ForeignKey(QueryMetrics.queryId), primary_key=True)
    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    operatorSummary = Column("operatorSummary", JSON(none_as_null=True), nullable=False)

    __table_args__ = {"extend_existing": True, "schema": "raw_metrics"}

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ColumnMetrics):
            return False
        return cast(bool, self.queryId == other.queryId and self.id == other.id)

    def __hash__(self) -> int:
        return hash(frozenset([self.queryId, self.id]))


class FailedEvent(Base):  # type: ignore

    __tablename__ = "failed_events"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    event = Column("event", UnicodeText, nullable=False)
    createTime = Column("createTime", DateTime, nullable=False, default=func.now())

    __table_args__ = {"extend_existing": True, "schema": "raw_metrics"}


def _get_datetime_from_field(field: str | int | float | datetime) -> datetime:
    if isinstance(field, float):
        return datetime.fromtimestamp(field)
    elif isinstance(field, int):
        return datetime.fromtimestamp(float(field))
    elif isinstance(field, str):
        return parser.parse(field)
    elif isinstance(field, datetime):
        return field
    else:
        raise TypeError(
            f"Invalid argument: {field=} should be a string, integer, float or datetime object, not {type(field)}"
        )


def _unique(dl: list[T]) -> list[T]:
    """
    Returns the set of unique elements from a list

    Comparing elements is done with: x == y iff hash(x) == hash(y)
    """
    return list(dict.fromkeys(dl))


def _load_json(s: str | bytes | None, *args: Any, **kwargs: Any) -> Any:
    """
    Wrapper aroud json.loads to handle None
    """
    if s:
        return loads(s, *args, **kwargs)
    else:
        return None


def get_query_metrics_from_raw(raw_metrics: dict[str, Any]) -> QueryMetrics:
    return QueryMetrics(
        queryId=raw_metrics["metadata"]["queryId"],
        transactionId=raw_metrics["metadata"].get("transactionId"),
        query=raw_metrics["metadata"].get("query"),
        uri=raw_metrics["metadata"].get("uri"),
        plan=raw_metrics["metadata"].get("plan"),
        payload=_load_json(raw_metrics["metadata"].get("payload")),
        queryType=raw_metrics["context"].get("queryType"),
        remoteClientAddress=raw_metrics["context"].get("remoteClientAddress"),
        user=raw_metrics["context"].get("user"),
        userAgent=raw_metrics["context"].get("userAgent"),
        source=raw_metrics["context"].get("source"),
        serverAddress=raw_metrics["context"].get("serverAddress"),
        serverVersion=raw_metrics["context"].get("serverVersion"),
        environment=raw_metrics["context"].get("environment"),
        sessionProperties=raw_metrics["context"].get("sessionProperties"),
        cpuTime=raw_metrics["statistics"].get("cpuTime"),
        failedCpuTime=raw_metrics["statistics"].get("failedCpuTime"),
        wallTime=raw_metrics["statistics"].get("wallTime"),
        queuedTime=raw_metrics["statistics"].get("queuedTime"),
        scheduledTime=raw_metrics["statistics"].get("scheduledTime"),
        failedScheduledTime=raw_metrics["statistics"].get("failedScheduledTime"),
        analysisTime=raw_metrics["statistics"].get("analysisTime"),
        planningTime=raw_metrics["statistics"].get("planningTime"),
        executionTime=raw_metrics["statistics"].get("executionTime"),
        inputBlockedTime=raw_metrics["statistics"].get("inputBlockedTime"),
        failedInputBlockedTime=raw_metrics["statistics"].get("failedInputBlockedTime"),
        outputBlockedTime=raw_metrics["statistics"].get("outputBlockedTime"),
        failedOutputBlockedTime=raw_metrics["statistics"].get("failedOutputBlockedTime"),
        peakUserMemoryBytes=raw_metrics["statistics"].get("peakUserMemoryBytes"),
        peakTotalNonRevocableMemoryBytes=raw_metrics["statistics"].get("peakTotalNonRevocableMemoryBytes"),
        peakTaskUserMemory=raw_metrics["statistics"].get("peakTaskUserMemory"),
        peakTaskTotalMemory=raw_metrics["statistics"].get("peakTaskTotalMemory"),
        physicalInputBytes=raw_metrics["statistics"].get("physicalInputBytes"),
        physicalInputRows=raw_metrics["statistics"].get("physicalInputRows"),
        internalNetworkBytes=raw_metrics["statistics"].get("internalNetworkBytes"),
        internalNetworkRows=raw_metrics["statistics"].get("internalNetworkRows"),
        totalBytes=raw_metrics["statistics"].get("totalBytes"),
        totalRows=raw_metrics["statistics"].get("totalRows"),
        outputBytes=raw_metrics["statistics"].get("outputBytes"),
        outputRows=raw_metrics["statistics"].get("outputRows"),
        writtenBytes=raw_metrics["statistics"].get("writtenBytes"),
        writtenRows=raw_metrics["statistics"].get("writtenRows"),
        processedInputBytes=raw_metrics["statistics"].get("processedInputBytes"),
        processedInputRows=raw_metrics["statistics"].get("processedInputRows"),
        cumulativeMemory=raw_metrics["statistics"].get("cumulativeMemory"),
        completedSplits=raw_metrics["statistics"].get("completedSplits"),
        resourceWaitingTime=raw_metrics["statistics"].get("resourceWaitingTime"),
        planNodeStatsAndCosts=_load_json(raw_metrics["statistics"].get("planNodeStatsAndCosts")),
        createTime=_get_datetime_from_field(raw_metrics["createTime"]),
        executionStartTime=_get_datetime_from_field(raw_metrics["executionStartTime"]),
        endTime=_get_datetime_from_field(raw_metrics["endTime"]),
    )


def get_column_metrics_from_raw(raw_metrics: dict[str, Any]) -> list[ColumnMetrics]:
    column_metrics = [
        ColumnMetrics(
            queryId=raw_metrics["metadata"]["queryId"],
            catalogName=table["catalogName"],
            schemaName=table["schema"],
            tableName=table["table"],
            columnName=column,
            physicalInputBytes=table.get("physicalInputBytes"),
            physicalInputRows=table.get("physicalInputRows"),
        )
        for table in raw_metrics["ioMetadata"].get("inputs", [])
        for column in table["columns"]
    ]

    # Trino may send a column multiple times with the same information
    return _unique(column_metrics)


def get_client_tags_from_raw(raw_metrics: dict[str, Any]) -> list[ClientTags]:
    client_tags = [
        ClientTags(queryId=raw_metrics["metadata"]["queryId"], clientTag=client_tag)
        for client_tag in raw_metrics["context"].get("clientTags", [])
    ]

    return _unique(client_tags)


def get_resource_groups_from_raw(raw_metrics: dict[str, Any]) -> list[ResourceGroups]:
    resource_groups = [
        ResourceGroups(queryId=raw_metrics["metadata"]["queryId"], resourceGroup=resource_group)
        for resource_group in raw_metrics["context"].get("resourceGroupId", [])
    ]

    return _unique(resource_groups)


def get_operator_summaries_from_raw(raw_metrics: dict[str, Any]) -> list[OperatorSummaries]:
    operator_summaries = [
        OperatorSummaries(queryId=raw_metrics["metadata"]["queryId"], operatorSummary=_load_json(operator_sumary))
        for operator_sumary in raw_metrics["statistics"].get("operatorSummaries", [])
    ]

    return _unique(operator_summaries)
