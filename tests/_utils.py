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
from json import loads
from uuid import uuid4

with open("tests/query_complete_event.json", "r") as f:
    _raw_metrics = loads(f.read())


def get_raw_metrics():
    _new_raw_metrics = deepcopy(_raw_metrics)
    _query_id = str(uuid4())
    _new_raw_metrics["metadata"]["queryId"] = _query_id
    return (_query_id, _new_raw_metrics)
