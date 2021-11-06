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
PENDING_SIZE: int = 1024 * 1024
BUFFER_SIZE: int = 32768
RECONNECT_TIME_WAIT: float = 2  # in seconds
MAX_RECONNECT_ATTEMPTS: int = 60
PING_INTERVAL: float = 120  # in seconds
MAX_OUTSTANDING_PINGS: int = 2
MAX_PAYLOAD_SIZE: int = 1048576
MAX_FLUSHER_QUEUE_SIZE: int = 1024
CONNECT_TIMEOUT: float = 2  # in seconds
DRAIN_TIMEOUT: float = 30  # in seconds

# Default Pending Limits of Subscriptions
SUB_PENDING_MSGS_LIMIT: int = 65536
SUB_PENDING_BYTES_LIMIT: int = 65536 * 1024

JS_API_PREFIX = "$JS.API"
