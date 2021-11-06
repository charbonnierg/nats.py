# Copyright 2021 - Guillaume Charbonnier
#
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
from __future__ import annotations

from base64 import b64decode
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

from nats.aio.js.models.base import parse_datetime


@dataclass
class Message:
    """A message read from a stream

    References:
        * Fetching a message from a stream by sequence - [NATS Docs](https://docs.nats.io/jetstream/nats_api_reference#fetching-from-a-stream-by-sequence)
    """

    subject: str
    seq: int
    time: datetime

    data: Optional[bytes] = None
    hdrs: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if isinstance(self.time, str):
            self.time = parse_datetime(self.time)

        if isinstance(self.data, str):
            self.data = b64decode(self.data)
