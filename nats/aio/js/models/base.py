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
from datetime import datetime, timezone


def parse_datetime(value: str) -> datetime:
    # Remove Z UTC marker
    if value.endswith("Z"):
        value = value[:-1]
    # Check precision
    if "." in value:
        base, microseconds = value.split(".")
        # We can keep up to 999999 microseconds
        value = base + "." + microseconds[:6]
    else:
        value = value + ".0"
    return datetime.strptime(value,
                             "%Y-%m-%dT%H:%M:%S.%f").astimezone(timezone.utc)


@dataclass
class JetStreamResponse:
    type: str
