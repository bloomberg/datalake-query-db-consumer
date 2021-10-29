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


from copy import deepcopy
from uuid import uuid4


def get_raw_metrics():
    _new_raw_metrics = deepcopy(_raw_metrics)
    _query_id = str(uuid4())
    _new_raw_metrics["metadata"]["queryId"] = _query_id
    return (_query_id, _new_raw_metrics)


_raw_metrics = {
    "metadata": {
        "queryId": "20210720_093342_00012_pzafy",
        "transactionId": "3e7820f0-4a9e-498f-88d8-c206ac85bd3e",
        "queryState": "FINISHED",
        "uri": "http://localhost:8080/v1/query/20210720_093342_00012_pzafy",
        "query": "SELECT * FROM table;",
    },
    "statistics": {
        "cpuTime": 25.975000000,
        "wallTime": 112.444000000,
        "queuedTime": 0.001000000,
        "scheduledTime": 156.696000000,
        "analysisTime": 12.178000000,
        "planningTime": 1.733000000,
        "executionTime": 100.266000000,
        "peakUserMemoryBytes": 24610191,
        "peakTotalNonRevocableMemoryBytes": 58178441,
        "peakTaskUserMemory": 19472880,
        "peakTaskTotalMemory": 43828971,
        "physicalInputBytes": 0,
        "physicalInputRows": 1293616,
        "internalNetworkBytes": 49808906,
        "internalNetworkRows": 1123864,
        "totalBytes": 0,
        "totalRows": 1293616,
        "outputBytes": 9019074,
        "outputRows": 160908,
        "writtenBytes": 0,
        "writtenRows": 0,
        "cumulativeMemory": 2.143774060571e12,
        "completedSplits": 507,
        "complete": True,
        "resourceWaitingTime": 12.178000000,
    },
    "context": {
        "user": "test-user",
        "principal": "test-user",
        "groups": [],
        "remoteClientAddress": "127.0.0.1",
        "userAgent": "StatementClientV1/8012395-dirty",
        "clientTags": [],
        "clientCapabilities": ["PATH", "PARAMETRIC_DATETIME"],
        "source": "datalake-cli",
        "resourceGroupId": ["global"],
        "sessionProperties": {},
        "resourceEstimates": {},
        "serverAddress": "localhost",
        "serverVersion": "dev",
        "environment": "test",
        "queryType": "SELECT",
    },
    "ioMetadata": {
        "inputs": [
            {
                "catalogName": "postgresql",
                "schema": "test-schema",
                "table": "test-table-1",
                "columns": ["test-column-1_1", "test-column-1_2"],
                "physicalInputBytes": 0,
                "physicalInputRows": 1135200,
            },
            {
                "catalogName": "postgresql",
                "schema": "test-schema",
                "table": "test-table-2",
                "columns": ["test-column-2_1"],
                "physicalInputBytes": 0,
                "physicalInputRows": 2,
            },
        ],
    },
    "warnings": [],
    "createTime": 1626773622,
    "executionStartTime": 1626773634,
    "endTime": 1626773734,
}
