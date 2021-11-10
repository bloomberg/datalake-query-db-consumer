import json
import logging
import os
from datetime import datetime
from queue import Queue
from threading import Event, Thread

import pytest
from confluent_kafka import KafkaException
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session, sessionmaker

from bloomberg.datalake.datalakequerydbconsumer._data_models import ColumnMetrics, QueryMetrics
from bloomberg.datalake.datalakequerydbconsumer._kafka import KafkaConsumer

from .._utils import get_raw_metrics
from ._kafka import KafkaProducer

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
def bad_handle_consumer():
    queue = Queue()
    _stop_event = Event()

    def _post_message_hook(message):
        _stop_event.set()
        queue.put(message)
        raise Exception("Emulate a processing error")

    _consumer = KafkaConsumer(_stop_event, _post_message_hook)
    _thread = Thread(target=_consumer.run)
    _thread.start()

    yield (queue, _consumer._consumer)

    queue.join()
    queue = None

    _stop_event.set()
    _thread.join()


@pytest.fixture()
def session():
    Session = sessionmaker(bind=engine)
    with Session() as ses:
        yield ses


@pytest.fixture()
def _cleanup(session: Session):

    yield

    session.query(ColumnMetrics).delete()
    session.query(QueryMetrics).delete()
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
        logging.debug("Got _message_key=%s which is not _query_id=%s", _message_key, _query_id)
        consumer_queue.task_done()
    else:
        consumer_queue.task_done()

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
    assert result.serverVersion == "dev"
    assert result.environment == "test"
    assert result.cpuTime == 25.975
    assert result.wallTime == 112.444
    assert result.queuedTime == 0.001
    assert result.scheduledTime == 156.696
    assert result.analysisTime == 12.178
    assert result.planningTime == 1.733
    assert result.executionTime == 100.266
    assert result.peakUserMemoryBytes == 24610191
    assert result.peakTotalNonRevocableMemoryBytes == 58178441
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


def test_fail_to_handle_dosent_commit(producer, bad_handle_consumer):
    # Given
    (_query_id, _raw_metrics) = get_raw_metrics()
    (_bad_handle_consumer_queue, _consumer) = bad_handle_consumer

    # When
    producer.enqueue_message(json.dumps(_raw_metrics).encode("utf-8"), _query_id)

    # Consumer throws an exception when it handles a message
    # This should mean that it can't commit the message
    _bad_handle_consumer_queue.get()
    _bad_handle_consumer_queue.task_done()

    # Then
    with pytest.raises(KafkaException, match=".*_NO_OFFSET.*"):
        _consumer.commit(asynchronous=False)
