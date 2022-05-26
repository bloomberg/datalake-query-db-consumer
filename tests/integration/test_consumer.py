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


import json
import logging
import os
from datetime import datetime
from queue import Queue
from threading import Event, Thread

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
from bloomberg.datalake.datalakequerydbconsumer._kafka import KafkaConsumer

from .._utils import get_raw_metrics, get_raw_metrics_with_output_sources
from ._kafka import KafkaProducer

logger = logging.getLogger(__name__)

DB_URL = os.environ.get("DATALAKEQUERYDBCONSUMER_DB_URL")

engine = create_engine(DB_URL)


@pytest.fixture()
def consumer_queue():
    _stop_event = Event()
    queue = Queue()

    def _post_message_hook(message):
        queue.put(message)

    _consumer = KafkaConsumer(_stop_event, _post_message_hook)
    _thread = Thread(target=_consumer.run)
    _thread.start()

    yield queue

    queue.join()
    queue = None

    _stop_event.set()
    _thread.join()
    _consumer._consumer.close()


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


@pytest.fixture(scope="module")
def producer():
    producer = KafkaProducer()
    yield producer

    producer.close()


@pytest.mark.usefixtures("_cleanup")
def test_consumer(producer, consumer_queue: Queue, session):
    # Given
    (_query_id, _raw_metrics) = get_raw_metrics()

    # When
    producer.enqueue_message(json.dumps(_raw_metrics).encode("utf-8"), _query_id)

    while (_message_key := consumer_queue.get().key()).decode("utf-8") != _query_id:
        logger.debug("Got _message_key=%s which is not _query_id=%s", _message_key, _query_id)
        consumer_queue.task_done()
    else:
        consumer_queue.task_done()

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

    result = session.query(ClientTags).order_by(ClientTags.clientTag.asc()).filter_by(queryId=_query_id).all()
    assert len(result) == 2
    client_tag_1, client_tag_2 = result
    assert client_tag_1.queryId == _query_id
    assert client_tag_1.clientTag == "load_balancer"
    assert client_tag_2.queryId == _query_id
    assert client_tag_2.clientTag == "superset"

    result = (
        session.query(ResourceGroups).order_by(ResourceGroups.resourceGroup.asc()).filter_by(queryId=_query_id).all()
    )
    assert len(result) == 2
    resource_group_1, resource_group_2 = result
    assert resource_group_1.queryId == _query_id
    assert resource_group_1.resourceGroup == "admin"
    assert resource_group_2.queryId == _query_id
    assert resource_group_2.resourceGroup == "global"

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
def test_fail_to_handle_saves_failed_event(producer, consumer_queue: Queue, session):
    # Given
    _query_id = "123"
    _event = '{"metadata": {"queryId": "1234", "query": "select * from table"}}'

    # When
    producer.enqueue_message(_event, _query_id)

    # Consummer will raise an Exception when proccessing
    while (_message_key := consumer_queue.get().key()).decode("utf-8") != _query_id:
        logging.debug("Got _message_key=%s which is not _query_id=%s", _message_key, _query_id)
        consumer_queue.task_done()
    else:
        consumer_queue.task_done()

    # Then
    result = session.query(FailedEvent).first()

    assert result is not None
    assert result.id is not None
    assert result.event == _event
    assert result.createTime is not None


@pytest.mark.usefixtures("_cleanup")
def test_fail_to_parse_saves_failed_event(producer, consumer_queue: Queue, session):
    # Given
    _query_id = "123"
    _event = "im_not_json"

    # When
    producer.enqueue_message(_event, _query_id)

    # Consummer will raise an Exception when proccessing
    while (_message_key := consumer_queue.get().key()).decode("utf-8") != _query_id:
        logger.debug("Got _message_key=%s which is not _query_id=%s", _message_key, _query_id)
        consumer_queue.task_done()
    else:
        consumer_queue.task_done()

    # Then
    result = session.query(FailedEvent).first()

    assert result is not None
    assert result.id is not None
    assert result.event == _event
    assert result.createTime is not None


@pytest.mark.usefixtures("_cleanup")
def test_consumer_with_output_sources(producer, consumer_queue: Queue, session):
    # Given
    (_query_id, _raw_metrics) = get_raw_metrics_with_output_sources()

    # When
    producer.enqueue_message(json.dumps(_raw_metrics).encode("utf-8"), _query_id)

    # Consummer will raise an Exception when proccessing
    while (_message_key := consumer_queue.get().key()).decode("utf-8") != _query_id:
        logger.debug("Got _message_key=%s which is not _query_id=%s", _message_key, _query_id)
        consumer_queue.task_done()
    else:
        consumer_queue.task_done()

    # Then
    # Check OutputColumns
    results = session.query(OutputColumn).all()

    assert len(results) == 3

    for result in results:
        assert result.queryId == _query_id
        assert result.catalogName == "hive"
        assert result.schemaName == "s"
        assert result.tableName == "t1"

    column_names = [result.columnName for result in results]
    expected_column_names = ["cnt", "page_url", "country"]
    assert len(column_names) == len(expected_column_names)
    assert sorted(column_names) == sorted(expected_column_names)

    # Check OutputColumnSources
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
