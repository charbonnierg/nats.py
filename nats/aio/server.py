# Copyright 2021 - Guillaume Charbonnier
#
# Copyright 2016-2018 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from dataclasses import dataclass
from typing import Any, List, Optional
from urllib.parse import ParseResult


class Srv:
    """
    Srv is a helper data structure to hold state of a server.
    """
    def __init__(self, uri: ParseResult) -> None:
        self.uri = uri
        self.reconnects = 0
        self.did_connect = False
        self.discovered = False
        self.last_attempt: Optional[float] = None
        self.tls_name: Optional[str] = None

    def __eq__(self, o: Any) -> bool:
        try:
            return self.uri == o.uri  # type: ignore[attr-defined, no-any-return]
        except AttributeError:
            return False


@dataclass
class SrvInfo:
    server_id: Optional[str] = None
    server_name: Optional[str] = None
    cluster: Optional[str] = None
    version: Optional[str] = None
    go: Optional[str] = None
    git_commit: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    max_payload: Optional[int] = None
    proto: Optional[int] = None
    client_id: Optional[int] = None
    client_ip: Optional[str] = None
    auth_required: Optional[bool] = None
    tls_required: Optional[bool] = None
    tls_verify: Optional[bool] = None
    connect_urls: Optional[List[str]] = None
    ldm: Optional[bool] = None
    jetstream: Optional[bool] = None
    domain: Optional[str] = None
    headers: Optional[bool] = None
    nonce: Optional[str] = None
