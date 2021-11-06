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
from typing import TYPE_CHECKING, Dict, Optional

from nats.aio.errors import JetStreamAPIError
from nats.protocol.constants import DESC_HDR, STATUS_HDR

if TYPE_CHECKING:
    from nats.aio.messages import Msg  # pragma: no cover


def check_js_headers(headers: Optional[Dict[str, str]]) -> None:
    if headers is None:
        return
    if STATUS_HDR in headers:
        code = headers[STATUS_HDR]
        if code[0] == "2":
            return
        desc = headers[DESC_HDR]
        raise JetStreamAPIError(code=code, description=desc)


def check_js_msg(msg: "Msg") -> None:
    if len(msg.data) == 0:
        check_js_headers(msg.headers)
