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
from dataclasses import dataclass
from typing import Optional

from .base import JetStreamResponse


@dataclass
class Limits:
    """Account limits

    References:
        * Multi-tenancy & Resource Mgmt, NATS Docs - https://docs.nats.io/jetstream/resource_management
    """

    max_memory: int
    max_storage: int
    max_streams: int
    max_consumers: int


@dataclass
class Api:
    """API stats"""

    total: int
    errors: int


@dataclass
class AccountInfo(JetStreamResponse):
    """Account information

    References:
        * Account Information, NATS Docs - https://docs.nats.io/jetstream/administration/account#account-information
    """

    memory: int
    storage: int
    streams: int
    consumers: int
    limits: Limits
    api: Api
    domain: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.limits, dict):
            self.limits = Limits(**self.limits)
        if isinstance(self.api, dict):
            self.api = Api(**self.api)
