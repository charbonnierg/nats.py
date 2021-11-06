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
import re

MSG_RE = re.compile(
    b'\\AMSG\\s+([^\\s]+)\\s+([^\\s]+)\\s+(([^\\s]+)[^\\S\r\n]+)?(\\d+)\r\n'
)
HMSG_RE = re.compile(
    b'\\AHMSG\\s+([^\\s]+)\\s+([^\\s]+)\\s+(([^\\s]+)[^\\S\r\n]+)?([\\d]+)\\s+(\\d+)\r\n'
)
OK_RE = re.compile(b'\\A\\+OK\\s*\r\n')
ERR_RE = re.compile(b'\\A-ERR\\s+(\'.+\')?\r\n')
PING_RE = re.compile(b'\\APING\\s*\r\n')
PONG_RE = re.compile(b'\\APONG\\s*\r\n')
INFO_RE = re.compile(b'\\AINFO\\s+([^\r\n]+)\r\n')

PROTOCOL = 1

INFO_OP = b'INFO'
CONNECT_OP = b'CONNECT'
PUB_OP = b'PUB'
HPUB_OP = b'HPUB'
MSG_OP = b'MSG'
HMSG_OP = b'HMSG'
SUB_OP = b'SUB'
UNSUB_OP = b'UNSUB'
PING_OP = b'PING'
PONG_OP = b'PONG'
OK_OP = b'+OK'
ERR_OP = b'-ERR'
MSG_END = b'\n'
_CRLF_ = b'\r\n'
_SPC_ = b' '
_EMPTY_ = b""

OK = OK_OP + _CRLF_
PING = PING_OP + _CRLF_
PONG = PONG_OP + _CRLF_
CRLF_SIZE = len(_CRLF_)
OK_SIZE = len(OK)
PING_SIZE = len(PING)
PONG_SIZE = len(PONG)
MSG_OP_SIZE = len(MSG_OP)
ERR_OP_SIZE = len(ERR_OP)

# States
AWAITING_CONTROL_LINE = 1
AWAITING_MSG_PAYLOAD = 2
MAX_CONTROL_LINE_SIZE = 1024

INBOX_PREFIX = bytearray(b'_INBOX.')

INBOX_PREFIX_LEN = len(INBOX_PREFIX) + 22 + 1

NATS_HDR_LINE = bytearray(b'NATS/1.0\r\n')

STATUS_MSG_LEN = 3  # e.g. 20x, 40x, 50x
CTRL_LEN = len(_CRLF_)

STATUS_HDR = "Status"
DESC_HDR = "Description"
LAST_CONSUMER_SEQ_HDR = "Nats-Last-Consumer"
LAST_STREAM_SEQ_HDR = "Nats-Last-Stream"

NO_MSGS_STATUS = "404"
CTRL_MSG_STATUS = "100"
NO_RESPONDERS_STATUS = "503"

ACK = b"+ACK"
NAK = b"-NAK"
WPI = b"+WPI"
TERM = b"+TERM"
