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
from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from nats.aio.errors import ErrNotJSMessage, NatsError
from nats.protocol.constants import ACK, NAK, TERM, WPI

if TYPE_CHECKING:
    from nats.aio.client import Client


class AckType(str, Enum):
    ACK = "ACK"
    NAK = "NAK"
    InProgress = "WPI"
    Term = "TERM"


_ACK_TYPES: Dict[str, bytes] = dict(
    ACK=ACK,
    NAK=NAK,
    InProgess=WPI,
    Term=TERM,
)

# $JS.ACK.<domain>.<account hash>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>.<a token with a random value>


class MsgMetadata:
    __slots__ = (
        'account_hash', 'domain', 'num_delivered', 'num_pending', 'timestamp',
        'stream', 'consumer', 'sequence'
    )

    class SequencePair:
        def __init__(
            self, consumer: Union[str, int, bytes], stream: Union[str, int,
                                                                  bytes]
        ) -> None:
            self.consumer = int(consumer)
            self.stream = int(stream)

    def __init__(
        self,
        sequence: Optional[SequencePair] = None,
        num_pending: Union[int, str, None] = None,
        num_delivered: Union[int, str, None] = None,
        timestamp: Optional[datetime] = None,
        stream: Optional[str] = None,
        consumer: Optional[str] = None,
        domain: Optional[str] = None,
        account_hash: Optional[str] = None,
    ) -> None:
        self.sequence = sequence
        self.num_pending = int(
            num_pending
        ) if num_pending is not None else num_pending
        self.num_delivered = int(
            num_delivered
        ) if num_delivered is not None else num_delivered
        self.timestamp = timestamp
        self.stream = stream
        self.consumer = consumer
        self.domain = domain
        self.account_hash = account_hash

    @staticmethod
    def _get_metadata_fields(reply: str) -> List[str]:
        if reply is None or reply == '':
            raise ErrNotJSMessage()
        tokens = reply.split('.')
        nb_tokens = len(tokens)
        if (nb_tokens != 9 and
                nb_tokens != 12) or tokens[0] != "$JS" or tokens[1] != "ACK":
            raise ErrNotJSMessage()
        return tokens

    @classmethod
    def from_tokens_v1(cls, tokens: List[str]) -> MsgMetadata:
        t = datetime.fromtimestamp(int(tokens[7]) / 1_000_000_000.0)
        return cls(
            sequence=MsgMetadata.SequencePair(tokens[5], tokens[6]),
            num_delivered=tokens[4],
            num_pending=tokens[8],
            timestamp=t,
            stream=tokens[2],
            consumer=tokens[3],
        )

    @classmethod
    def from_tokens_v2(cls, tokens: List[str]) -> MsgMetadata:
        """Source: https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-15.md#v2-notes"""
        t = datetime.fromtimestamp(int(tokens[9]) / 1_000_000_000.0)
        domain = tokens[2] if tokens[2] != "_" else None
        return cls(
            account_hash=tokens[3],
            stream=tokens[4],
            consumer=tokens[5],
            num_delivered=tokens[6],
            sequence=MsgMetadata.SequencePair(tokens[7], tokens[8]),
            num_pending=tokens[10],
            timestamp=t,
            domain=domain,
        )

    @classmethod
    def from_subject(cls, subject: str) -> MsgMetadata:
        if subject is None or subject == '':
            raise ErrNotJSMessage()
        tokens = subject.split('.')
        try:
            if tokens[0] != "$JS" or tokens[1] != "ACK":
                raise ErrNotJSMessage()
        except Exception:
            raise ErrNotJSMessage()
        nb_tokens = len(tokens)
        if nb_tokens == 9:
            return cls.from_tokens_v1(tokens)
        elif nb_tokens == 12:
            return cls.from_tokens_v2(tokens)
        else:
            raise ErrNotJSMessage()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: stream='{self.stream}' consumer='{self.consumer}' sequence=({self.sequence.stream if self.sequence else None}, {self.sequence.consumer if self.sequence else None})>"


class Msg:
    """
    Msg represents a message delivered by NATS.
    """
    __slots__ = (
        'subject', 'reply', 'data', 'sid', 'headers', '_client', '_metadata'
    )

    def __init__(
        self,
        subject: str = '',
        reply: str = '',
        data: bytes = b'',
        sid: int = 0,
        client: Optional["Client"] = None,
        headers: Dict[str, str] = None,
    ) -> None:
        self.subject = subject
        self.reply = reply
        self.data = data
        self.sid = sid
        self._client = client
        self.headers = headers
        self._metadata: Optional[MsgMetadata] = None

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: subject='{self.subject}' reply='{self.reply}' headers={self.headers} data='{self.data[:10].decode()}...'>"

    async def respond(self, data: bytes = b"") -> None:
        if not self.reply:
            raise NatsError('no reply subject available')
        if not self._client:
            raise NatsError('client not set')

        await self._client.publish(self.reply, data, headers=self.headers)

    async def respond_sync(
        self, data: bytes = b"", timeout: float = 1
    ) -> "Msg":
        if not self.reply:
            raise NatsError('no reply subject available')
        if not self._client:
            raise NatsError('client not set')

        return await self._client.request(
            self.reply, data, timeout=timeout, headers=self.headers
        )

    async def ack(self, kind: AckType = AckType.ACK) -> None:
        """
        ack acknowledges a message delivered by JetStream.
        """
        if not self._client:
            raise NatsError('client not set')
        if self.reply is None or self.reply == '':
            raise ErrNotJSMessage
        await self.respond(_ACK_TYPES[AckType(kind).value])

    async def ack_sync(
        self, kind: AckType = AckType.ACK, timeout: float = 1.0
    ) -> None:
        """
        ack_sync waits for the acknowledgement to be processed by the server.
        """
        if not self._client:
            raise NatsError('client not set')
        if self.reply is None or self.reply == '':
            raise ErrNotJSMessage
        await self.respond_sync(
            _ACK_TYPES[AckType(kind).value], timeout=timeout
        )

    async def nak(self) -> None:
        await self.ack(AckType.NAK)

    async def term(self) -> None:
        await self.ack(AckType.Term)

    async def in_progress(self) -> None:
        await self.ack(AckType.InProgress)

    @property
    def metadata(self) -> MsgMetadata:
        """
        metadata returns the metadata from a JetStream message
        """
        if self._metadata is not None:
            return self._metadata
        self._metadata = MsgMetadata.from_subject(self.reply)
        return self._metadata
