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


import os
from datetime import datetime

import pytest
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session, sessionmaker

from bloomberg.datalake.datalakequerydbconsumer._data_models import (
    ClientTags,
    ColumnMetrics,
    FailedEvent,
    OperatorSummaries,
    OutputColumn,
    OutputColumnSource,
    QueryMetrics,
    ResourceGroups,
)
from bloomberg.datalake.datalakequerydbconsumer._db_accessor import (
    _add_client_tags,
    _add_column_metrics,
    _add_failed_event,
    _add_operator_summaries,
    _add_output_column_sources,
    _add_output_columns,
    _add_query_metrics,
    _add_resource_groups,
)

from .._utils import get_raw_metrics, get_raw_metrics_with_no_sources, get_raw_metrics_with_output_sources

DB_URL = os.environ.get("DATALAKEQUERYDBCONSUMER_DB_URL")

engine = create_engine(DB_URL)


@pytest.fixture()
def session():
    Session = sessionmaker(bind=engine)
    with Session() as ses:
        yield ses


@pytest.fixture()
def _cleanup(session: Session):

    yield

    session.query(OutputColumnSource).delete()
    session.query(OutputColumn).delete()
    session.query(ClientTags).delete()
    session.query(ResourceGroups).delete()
    session.query(OperatorSummaries).delete()
    session.query(ColumnMetrics).delete()
    session.query(QueryMetrics).delete()
    session.query(FailedEvent).delete()
    session.commit()


@pytest.mark.usefixtures("_cleanup")
def test_add_query_metrics(session: Session):
    # Given
    (_query_id, _raw_metrics) = get_raw_metrics()

    # When
    _add_query_metrics(_raw_metrics)

    # Then
    result = session.query(QueryMetrics).filter_by(queryId=_query_id).first()

    assert result is not None
    assert result.queryId == _query_id
    assert result.transactionId == "3e7820f0-4a9e-498f-88d8-c206ac85bd3e"
    assert result.query == "SELECT * FROM table;"
    assert result.uri == "http://localhost:8080/v1/query/20210720_093342_00012_pzafy"
    assert "Fragment 0" in result.plan
    assert "└─" in result.plan  # Ensure unicode characters are persisted correctly
    assert result.payload["stageId"] == "20220505_100804_00003_7dpwd.0"
    assert result.queryType == "SELECT"
    assert result.remoteClientAddress == "127.0.0.1"
    assert result.user == "test-user"
    assert result.userAgent == "StatementClientV1/8012395-dirty"
    assert result.source == "datalake-cli"
    assert result.serverAddress == "localhost"
    assert result.serverVersion == "379"
    assert result.environment == "test"
    assert result.sessionProperties == {"catalog": "postgresql"}
    assert result.cpuTime == 25.975
    assert result.failedCpuTime == 0
    assert result.wallTime == 112.444
    assert result.queuedTime == 0.001
    assert result.scheduledTime == 156.696
    assert result.failedScheduledTime == 1
    assert result.analysisTime == 12.178
    assert result.planningTime == 1.733
    assert result.executionTime == 100.266
    assert result.inputBlockedTime == 7.72
    assert result.failedInputBlockedTime == 0.1
    assert result.outputBlockedTime == 0.2
    assert result.failedOutputBlockedTime == 0.3
    assert result.peakUserMemoryBytes == 24610191
    assert result.peakTaskUserMemory == 19472880
    assert result.peakTaskTotalMemory == 43828971
    assert result.physicalInputBytes == 0
    assert result.physicalInputRows == 1293616
    assert result.internalNetworkBytes == 49808906
    assert result.internalNetworkRows == 1123864
    assert result.totalBytes == 0
    assert result.totalRows == 1293616
    assert result.outputBytes == 9019074
    assert result.outputRows == 160908
    assert result.writtenBytes == 0
    assert result.writtenRows == 0
    assert result.processedInputRows == 1
    assert result.processedInputBytes == 571
    assert result.cumulativeMemory == 2143774060571.0
    assert result.completedSplits == 507
    assert result.resourceWaitingTime == 12.178
    assert result.planNodeStatsAndCosts == {"stats": {}, "costs": {}}
    assert result.createTime == datetime.fromtimestamp(1626773622)
    assert result.executionStartTime == datetime.fromtimestamp(1626773634)
    assert result.endTime == datetime.fromtimestamp(1626773734)


@pytest.mark.usefixtures("_cleanup")
def test_add_older_version_query_metrics(session: Session):
    # Given
    (_query_id, _raw_metrics) = get_raw_metrics()
    del _raw_metrics["statistics"]["processedInputBytes"]
    del _raw_metrics["statistics"]["processedInputRows"]
    _raw_metrics["statistics"]["someOldNotUsedValue"] = 1337

    # When
    _add_query_metrics(_raw_metrics)

    # Then
    result = session.query(QueryMetrics).filter_by(queryId=_query_id).first()

    assert result is not None
    assert result.queryId == _query_id


@pytest.mark.usefixtures("_cleanup")
def test_add_query_metrics_missing_json_stringify_fields(session: Session):
    # Given
    (_query_id, _raw_metrics) = get_raw_metrics()
    del _raw_metrics["metadata"]["payload"]
    del _raw_metrics["statistics"]["planNodeStatsAndCosts"]

    # When
    _add_query_metrics(_raw_metrics)

    # Then
    result = session.query(QueryMetrics).filter_by(queryId=_query_id).first()

    assert result is not None
    assert result.queryId == _query_id


@pytest.mark.usefixtures("_cleanup")
def test_add_column_metrics(session: Session):
    # Given
    (_query_id, _raw_metrics) = get_raw_metrics()

    # When
    _add_query_metrics(_raw_metrics)  # Needed for FK
    _add_column_metrics(_raw_metrics)

    # Then
    result = (
        session.query(ColumnMetrics)
        .order_by(ColumnMetrics.catalogName.asc())
        .order_by(ColumnMetrics.schemaName.asc())
        .order_by(ColumnMetrics.tableName.asc())
        .order_by(ColumnMetrics.columnName.asc())
        .filter_by(queryId=_query_id)
        .all()
    )

    assert len(result) == 3

    assert result[0].queryId == _query_id
    assert result[0].catalogName == "postgresql"
    assert result[0].schemaName == "test-schema"
    assert result[0].tableName == "test-table-1"
    assert result[0].columnName == "test-column-1_1"
    assert result[0].physicalInputBytes == 0
    assert result[0].physicalInputRows == 1135200

    assert result[1].queryId == _query_id
    assert result[1].catalogName == "postgresql"
    assert result[1].schemaName == "test-schema"
    assert result[1].tableName == "test-table-1"
    assert result[1].columnName == "test-column-1_2"
    assert result[1].physicalInputBytes == 0
    assert result[1].physicalInputRows == 1135200

    assert result[2].queryId == _query_id
    assert result[2].catalogName == "postgresql"
    assert result[2].schemaName == "test-schema"
    assert result[2].tableName == "test-table-2"
    assert result[2].columnName == "test-column-2_1"
    assert result[2].physicalInputBytes == 0
    assert result[2].physicalInputRows == 2


@pytest.mark.usefixtures("_cleanup")
def test_str_or_float_timestamps(session: Session):
    # Given
    (_query_id, _raw_metrics) = get_raw_metrics()
    _raw_metrics["createTime"] = "2021-07-20T9:33:42.000Z"
    _raw_metrics["endTime"] = 1626773734.0

    # When
    _add_query_metrics(_raw_metrics)

    # Then
    result = session.query(QueryMetrics).filter_by(queryId=_query_id).first()

    assert result is not None
    assert result.queryId == _query_id
    assert result.transactionId == "3e7820f0-4a9e-498f-88d8-c206ac85bd3e"
    assert result.query == "SELECT * FROM table;"
    assert result.queryType == "SELECT"
    assert result.remoteClientAddress == "127.0.0.1"
    assert result.user == "test-user"
    assert result.userAgent == "StatementClientV1/8012395-dirty"
    assert result.source == "datalake-cli"
    assert result.serverAddress == "localhost"
    assert result.serverVersion == "379"
    assert result.environment == "test"
    assert result.cpuTime == 25.975
    assert result.wallTime == 112.444
    assert result.queuedTime == 0.001
    assert result.scheduledTime == 156.696
    assert result.analysisTime == 12.178
    assert result.planningTime == 1.733
    assert result.executionTime == 100.266
    assert result.peakUserMemoryBytes == 24610191
    assert result.peakTaskUserMemory == 19472880
    assert result.peakTaskTotalMemory == 43828971
    assert result.physicalInputBytes == 0
    assert result.physicalInputRows == 1293616
    assert result.internalNetworkBytes == 49808906
    assert result.internalNetworkRows == 1123864
    assert result.totalBytes == 0
    assert result.totalRows == 1293616
    assert result.outputBytes == 9019074
    assert result.outputRows == 160908
    assert result.writtenBytes == 0
    assert result.writtenRows == 0
    assert result.cumulativeMemory == 2143774060571.0
    assert result.completedSplits == 507
    assert result.resourceWaitingTime == 12.178
    assert result.createTime == datetime.fromtimestamp(1626773622)
    assert result.executionStartTime == datetime.fromtimestamp(1626773634)
    assert result.endTime == datetime.fromtimestamp(1626773734)


@pytest.mark.usefixtures("_cleanup")
def test_duplicate_columns(session: Session):
    # Given
    (_query_id, _raw_metrics) = get_raw_metrics()
    # Duplicate first input table
    _raw_metrics["ioMetadata"]["inputs"].append(_raw_metrics["ioMetadata"]["inputs"][0])

    # When
    _add_query_metrics(_raw_metrics)
    _add_column_metrics(_raw_metrics)

    # Then
    result = (
        session.query(ColumnMetrics)
        .order_by(ColumnMetrics.catalogName.asc())
        .order_by(ColumnMetrics.schemaName.asc())
        .order_by(ColumnMetrics.tableName.asc())
        .order_by(ColumnMetrics.columnName.asc())
        .filter_by(queryId=_query_id)
        .all()
    )

    assert len(result) == 3

    assert result[0].queryId == _query_id
    assert result[0].catalogName == "postgresql"
    assert result[0].schemaName == "test-schema"
    assert result[0].tableName == "test-table-1"
    assert result[0].columnName == "test-column-1_1"
    assert result[0].physicalInputBytes == 0
    assert result[0].physicalInputRows == 1135200

    assert result[1].queryId == _query_id
    assert result[1].catalogName == "postgresql"
    assert result[1].schemaName == "test-schema"
    assert result[1].tableName == "test-table-1"
    assert result[1].columnName == "test-column-1_2"
    assert result[1].physicalInputBytes == 0
    assert result[1].physicalInputRows == 1135200

    assert result[2].queryId == _query_id
    assert result[2].catalogName == "postgresql"
    assert result[2].schemaName == "test-schema"
    assert result[2].tableName == "test-table-2"
    assert result[2].columnName == "test-column-2_1"
    assert result[2].physicalInputBytes == 0
    assert result[2].physicalInputRows == 2


@pytest.mark.usefixtures("_cleanup")
def test_add_client_tags(session: Session):
    # Given
    (_query_id, _raw_metrics) = get_raw_metrics()

    # When
    _add_query_metrics(_raw_metrics)  # Needed for FK
    _add_client_tags(_raw_metrics)

    # Then
    result = session.query(ClientTags).order_by(ClientTags.clientTag.asc()).filter_by(queryId=_query_id).all()

    assert len(result) == 2

    client_tag_1, client_tag_2 = result

    assert client_tag_1.queryId == _query_id
    assert client_tag_1.clientTag == "load_balancer"

    assert client_tag_2.queryId == _query_id
    assert client_tag_2.clientTag == "superset"


@pytest.mark.usefixtures("_cleanup")
def test_add_resource_groups(session: Session):
    # Given
    (_query_id, _raw_metrics) = get_raw_metrics()

    # When
    _add_query_metrics(_raw_metrics)  # Needed for FK
    _add_resource_groups(_raw_metrics)

    # Then
    result = (
        session.query(ResourceGroups).order_by(ResourceGroups.resourceGroup.asc()).filter_by(queryId=_query_id).all()
    )

    assert len(result) == 2

    resource_group_1, resource_group_2 = result

    assert resource_group_1.queryId == _query_id
    assert resource_group_1.resourceGroup == "admin"

    assert resource_group_2.queryId == _query_id
    assert resource_group_2.resourceGroup == "global"


@pytest.mark.usefixtures("_cleanup")
def test_add_operator_summaries(session: Session):
    # Given
    (_query_id, _raw_metrics) = get_raw_metrics()

    # When
    _add_query_metrics(_raw_metrics)  # Needed for FK
    _add_operator_summaries(_raw_metrics)

    # Then
    result = (
        session.query(OperatorSummaries)
        .order_by(OperatorSummaries.operatorSummary["stageId"].as_integer().asc())
        .order_by(OperatorSummaries.operatorSummary["pipelineId"].as_integer().asc())
        .order_by(OperatorSummaries.operatorSummary["operatorId"].as_integer().asc())
        .order_by(OperatorSummaries.operatorSummary["planNodeId"].as_integer().asc())
        .filter_by(queryId=_query_id)
        .all()
    )

    assert len(result) == 4

    assert result[0].operatorSummary["stageId"] == 0
    assert result[0].operatorSummary["pipelineId"] == 0
    assert result[0].operatorSummary["operatorId"] == 0
    assert result[0].operatorSummary["planNodeId"] == "89"

    assert result[1].operatorSummary["stageId"] == 0
    assert result[1].operatorSummary["pipelineId"] == 0
    assert result[1].operatorSummary["operatorId"] == 1
    assert result[1].operatorSummary["planNodeId"] == "6"

    assert result[2].operatorSummary["stageId"] == 1
    assert result[2].operatorSummary["pipelineId"] == 0
    assert result[2].operatorSummary["operatorId"] == 0
    assert result[2].operatorSummary["planNodeId"] == "0"

    assert result[3].operatorSummary["stageId"] == 1
    assert result[3].operatorSummary["pipelineId"] == 0
    assert result[3].operatorSummary["operatorId"] == 1
    assert result[3].operatorSummary["planNodeId"] == "0"


@pytest.mark.usefixtures("_cleanup")
def test_add_failed_event(session: Session):
    # Given
    _event = '{"metadata": {"queryId": "1234", "query": "select * from table"}}'

    # When
    _add_failed_event(_event)

    # Then
    result = session.query(FailedEvent).first()

    assert result is not None
    assert result.id is not None
    assert result.event == _event
    assert result.createTime is not None


@pytest.mark.usefixtures("_cleanup")
def test_add_output_columns(session: Session):
    # Given
    (_query_id, _raw_metrics) = get_raw_metrics_with_no_sources()

    # When
    _add_query_metrics(_raw_metrics)  # Needed for FK
    _add_output_columns(_raw_metrics)

    # Then
    results = session.query(OutputColumn).all()

    assert len(results) == 5

    for result in results:
        assert result.queryId == _query_id
        assert result.catalogName == "hive"
        assert result.schemaName == "s"
        assert result.tableName == "t"

    column_names = [result.columnName for result in results]
    expected_column_names = ["view_time", "user_id", "page_url", "ds", "country"]
    assert len(column_names) == len(expected_column_names)
    assert sorted(column_names) == sorted(expected_column_names)


@pytest.mark.usefixtures("_cleanup")
def test_add_output_column_sources(session: Session):
    # Given
    (_query_id, _raw_metrics) = get_raw_metrics_with_output_sources()

    # When
    _add_query_metrics(_raw_metrics)
    _add_output_columns(_raw_metrics)
    _add_output_column_sources(_raw_metrics)

    # Then
    results = session.query(OutputColumnSource).order_by(OutputColumnSource.sourceColumnName.asc()).all()

    assert len(results) == 2

    for result in results:
        assert result.queryId == _query_id
        assert result.catalogName == "hive"
        assert result.schemaName == "s"
        assert result.tableName == "t1"
        assert result.sourceCatalogName == "hive"
        assert result.sourceSchemaName == "s"
        assert result.sourceTableName == "t"

    assert results[0].columnName == "country"
    assert results[0].sourceColumnName == "country"

    assert results[1].columnName == "page_url"
    assert results[1].sourceColumnName == "page_url"
