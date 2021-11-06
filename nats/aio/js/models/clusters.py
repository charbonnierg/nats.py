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

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Replica:
    """Peer info."""

    name: str
    current: bool
    active: float
    offline: Optional[bool] = False
    lag: Optional[int] = None


@dataclass
class Cluster:
    """Cluster info."""

    name: Optional[str] = None
    leader: Optional[str] = None
    replicas: Optional[List[Replica]] = None
