# Copyright 2021 The NATS Authors
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
#

from __future__ import annotations

import asyncio
import base64
import json
from email.parser import BytesParser
from typing import TYPE_CHECKING, Any, Awaitable, Callable, List, Optional, Dict, overload

from nats.errors import NoRespondersError
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js import api
from nats.js.errors import APIError, NotFoundError, ServiceUnavailableError

if TYPE_CHECKING:
    from nats import NATS

NATS_HDR_LINE = bytearray(b'NATS/1.0')
NATS_HDR_LINE_SIZE = len(NATS_HDR_LINE)
_CRLF_ = b'\r\n'
_CRLF_LEN_ = len(_CRLF_)


class JetStreamManager:
    """
    JetStreamManager exposes management APIs for JetStream.
    """

    def __init__(
        self,
        conn: NATS,
        prefix: str = api.DEFAULT_PREFIX,
        timeout: float = 5,
    ) -> None:
        self._prefix = prefix
        self._nc = conn
        self._timeout = timeout
        self._hdr_parser = BytesParser()

    async def account_info(self) -> api.AccountInfo:
        resp = await self._api_request(
            f"{self._prefix}.INFO", b'', timeout=self._timeout
        )
        return api.AccountInfo.from_response(resp)

    async def find_stream_name_by_subject(self, subject: str) -> str:
        """
        Find the stream to which a subject belongs in an account.
        """

        req_sub = f"{self._prefix}.STREAM.NAMES"
        req_data = json.dumps({"subject": subject})
        info = await self._api_request(
            req_sub, req_data.encode(), timeout=self._timeout
        )
        if not info['streams']:
            raise NotFoundError
        return info['streams'][0]

    async def stream_info(self, name: str, subjects_filter: Optional[str] = None) -> api.StreamInfo:
        """
        Get the latest StreamInfo by stream name.
        """
        req_data = ''
        if subjects_filter:
            req_data = json.dumps({"subjects_filter": subjects_filter})
        resp = await self._api_request(
            f"{self._prefix}.STREAM.INFO.{name}", req_data.encode(), timeout=self._timeout
        )
        return api.StreamInfo.from_response(resp)

    async def add_stream(
        self,
        config: Optional[api.StreamConfig] = None,
        **params
    ) -> api.StreamInfo:
        """
        add_stream creates a stream.
        """
        if config is None:
            config = api.StreamConfig()
        config = config.evolve(**params)
        if config.name is None:
            raise ValueError("nats: stream name is required")

        data = json.dumps(config.as_dict())
        resp = await self._api_request(
            f"{self._prefix}.STREAM.CREATE.{config.name}",
            data.encode(),
            timeout=self._timeout,
        )
        return api.StreamInfo.from_response(resp)

    async def update_stream(
        self,
        config: Optional[api.StreamConfig] = None,
        **params
    ) -> api.StreamInfo:
        """
        update_stream updates a stream.
        """
        if config is None:
            config = api.StreamConfig()
        config = config.evolve(**params)
        if config.name is None:
            raise ValueError("nats: stream name is required")

        data = json.dumps(config.as_dict())
        resp = await self._api_request(
            f"{self._prefix}.STREAM.UPDATE.{config.name}",
            data.encode(),
            timeout=self._timeout,
        )
        return api.StreamInfo.from_response(resp)

    async def delete_stream(self, name: str) -> bool:
        """
        Delete a stream by name.
        """
        resp = await self._api_request(
            f"{self._prefix}.STREAM.DELETE.{name}", timeout=self._timeout
        )
        return resp['success']

    async def purge_stream(
        self,
        name: str,
        seq: Optional[int] = None,
        subject: Optional[str] = None,
        keep: Optional[int] = None
    ) -> bool:
        """
        Purge a stream by name.
        """
        stream_req: Dict[str, Any] = {}
        if seq:
            stream_req['seq'] = seq
        if subject:
            stream_req['filter'] = subject
        if keep:
            stream_req['keep'] = keep

        req = json.dumps(stream_req)
        resp = await self._api_request(
            f"{self._prefix}.STREAM.PURGE.{name}",
            req.encode(),
            timeout=self._timeout
        )
        return resp['success']

    async def consumer_info(
        self, stream: str, consumer: str, timeout: Optional[float] = None
    ):
        # TODO: Validate the stream and consumer names.
        if timeout is None:
            timeout = self._timeout
        resp = await self._api_request(
            f"{self._prefix}.CONSUMER.INFO.{stream}.{consumer}",
            b'',
            timeout=timeout
        )
        return api.ConsumerInfo.from_response(resp)

    async def streams_info(self) -> List[api.StreamInfo]:
        """
        streams_info retrieves a list of streams.
        """
        resp = await self._api_request(
            f"{self._prefix}.STREAM.LIST",
            b'',
            timeout=self._timeout,
        )
        streams = []
        for stream in resp['streams']:
            stream_info = api.StreamInfo.from_response(stream)
            streams.append(stream_info)
        return streams

    async def add_consumer(
        self,
        stream: str,
        config: Optional[api.ConsumerConfig] = None,
        timeout: Optional[float] = None,
        **params,
    ) -> api.ConsumerInfo:
        if not timeout:
            timeout = self._timeout
        if config is None:
            config = api.ConsumerConfig()
        config = config.evolve(**params)
        durable_name = config.durable_name
        req = {"stream_name": stream, "config": config.as_dict()}
        req_data = json.dumps(req).encode()

        resp = None
        subject = ''
        version = self._nc.connected_server_version
        consumer_name_supported = version.major >= 2 and version.minor >= 9
        if consumer_name_supported and config.name:
            # NOTE: Only supported after nats-server v2.9.0
            if config.filter_subject and config.filter_subject != ">":
                subject = f"{self._prefix}.CONSUMER.CREATE.{stream}.{config.name}.{config.filter_subject}"
            else:
                subject = f"{self._prefix}.CONSUMER.CREATE.{stream}.{config.name}"
        elif durable_name:
            # NOTE: Legacy approach to create consumers. After nats-server v2.9
            # name option can be used instead.
            subject = f"{self._prefix}.CONSUMER.DURABLE.CREATE.{stream}.{durable_name}"
        else:
            subject = f"{self._prefix}.CONSUMER.CREATE.{stream}"

        resp = await self._api_request(subject, req_data, timeout=timeout)
        return api.ConsumerInfo.from_response(resp)

    async def delete_consumer(self, stream: str, consumer: str) -> bool:
        resp = await self._api_request(
            f"{self._prefix}.CONSUMER.DELETE.{stream}.{consumer}",
            b'',
            timeout=self._timeout
        )
        return resp['success']

    async def consumers_info(
        self,
        stream: str,
        offset: Optional[int] = None
    ) -> List[api.ConsumerInfo]:
        """
        consumers_info retrieves a list of consumers. Consumers list limit is 256 for more
        consider to use offset
        :param stream: stream to get consumers
        :param offset: consumers list offset
        """
        resp = await self._api_request(
            f"{self._prefix}.CONSUMER.LIST.{stream}",
            b'' if offset is None else json.dumps({
                "offset": offset
            }).encode(),
            timeout=self._timeout,
        )
        consumers = []
        for consumer in resp['consumers']:
            consumer_info = api.ConsumerInfo.from_response(consumer)
            consumers.append(consumer_info)
        return consumers

    @overload
    def direct_get(
        self,
        stream: str,
        *,
        batch_size: int,
        seq: int | None = None,
        next_by_subj: str | None = None,
        max_bytes: int | None = None,
        # FIXME: Until there is support for RFC3339 in nats.py, force
        # users to use strings. This is sub-optimal, but it's better
        # than nothing.
        start_time: str | None = None,
        multi_last: list[str] | None = None,
        up_to_seq: int | None = None,
        up_to_time: str | None = None,
        continue_from: api.DirectGetResult | None = None,
        continue_on_eob: bool = False,
    ) -> DirectGetResponse: ...

    @overload
    def direct_get(
        self,
        stream: str,
        *,
        cb: Callable[[api.RawStreamMsg], Awaitable[None]],
        batch_size: int,
        seq: int | None = None,
        next_by_subj: str | None = None,
        max_bytes: int | None = None,
        # FIXME: Until there is support for RFC3339 in nats.py, force
        # users to use strings. This is sub-optimal, but it's better
        # than nothing.
        start_time: str | None = None,
        multi_last: list[str] | None = None,
        up_to_seq: int | None = None,
        up_to_time: str | None = None,
        continue_from: api.DirectGetResult | None = None,
        continue_on_eob: bool = False,
    ) -> Awaitable[api.DirectGetResult]: ...

    def direct_get(
        self,
        stream: str,
        *,
        batch_size: int,
        cb: Callable[[api.RawStreamMsg], Awaitable[None]] | None = None,
        seq: int | None = None,
        next_by_subj: str | None = None,
        max_bytes: int | None = None,
        # FIXME: Until there is support for RFC3339 in nats.py, force
        # users to use strings. This is sub-optimal, but it's better
        # than nothing.
        start_time: str | None = None,
        multi_last: list[str] | None = None,
        up_to_seq: int | None = None,
        up_to_time: str | None = None,
        continue_from: api.DirectGetResult | None = None,
        continue_on_eob: bool = False,
    ) -> DirectGetResponse | Awaitable[api.DirectGetResult]:
        """Send a Direct Get request.

        NOTE: The start_time and up_to_time parameters must be valid RFC3339 timestamps as a string. This may
        change in the future to support datetime objects.

        :param seq: Stream sequence number of the message to retrieve, cannot be combined with last_by_subj.
        :param next_by_subj: Combined with sequence gets the next message for a subject with the given sequence or higher.
        :param batch: Request a number of messages to be delivered.
        :param max_bytes: Restrict batch get to a certain maximum cumulative bytes, defaults to server MAX_PENDING_SIZE.
        :param start_time: Start the batch at a certain point in time rather than sequence. A point in time in RFC3339 format including timezone, though typically in UTC.
        :param multi_last: Get the last messages from the supplied subjects.
        :param up_to_seq: Returns messages up to this sequence otherwise last sequence for the stream.
        :param up_to_time: Only return messages up to a point in time. A point in time in RFC3339 format including timezone, though typically in UTC.
        """
        version = self._nc.connected_server_version
        batch_direct_get_supported = version.major >= 2 and version.minor >= 11
        if not batch_direct_get_supported:
            raise RuntimeError("nats-server < 2.11 does not support direct batch")
        if continue_from:
            seq = continue_from.last_seq + 1
            up_to_seq = continue_from.up_to_seq
        if cb:
            return _direct_get(
                self,
                stream=stream,
                cb=cb,
                batch=batch_size,
                seq=seq,
                next_by_subj=next_by_subj,
                max_bytes=max_bytes,
                start_time=start_time,
                multi_last=multi_last,
                up_to_seq=up_to_seq,
                up_to_time=up_to_time,
                continue_on_eob=continue_on_eob,
            )
        return DirectGetResponse(
            self,
            stream=stream,
            batch=batch_size,
            seq=seq,
            next_by_subj=next_by_subj,
            max_bytes=max_bytes,
            start_time=start_time,
            multi_last=multi_last,
            up_to_seq=up_to_seq,
            up_to_time=up_to_time,
            continue_on_eob=continue_on_eob,
        )

    async def get_msg(
        self,
        stream_name: str,
        seq: Optional[int] = None,
        subject: Optional[str] = None,
        direct: Optional[bool] = False,
        next: Optional[bool] = False,
    ) -> api.RawStreamMsg:
        """
        get_msg retrieves a message from a stream.
        """
        req_subject = None
        req: Dict[str, Any] = {}
        if seq:
            req['seq'] = seq
        if subject:
            req['seq'] = None
            req.pop('seq', None)
            req['last_by_subj'] = subject
        if next:
            req['seq'] = seq
            req['last_by_subj'] = None
            req.pop('last_by_subj', None)
            req['next_by_subj'] = subject
        data = json.dumps(req)

        if direct:
            # $JS.API.DIRECT.GET.KV_{stream_name}.$KV.TEST.{key}
            if subject and not seq:
                # last_by_subject type request requires no payload.
                data = ''
                req_subject = f"{self._prefix}.DIRECT.GET.{stream_name}.{subject}"
            else:
                req_subject = f"{self._prefix}.DIRECT.GET.{stream_name}"

            resp = await self._nc.request(
                req_subject, data.encode(), timeout=self._timeout
            )
            raw_msg = JetStreamManager._lift_msg_to_raw_msg(resp)
            return raw_msg

        # Non Direct form
        req_subject = f"{self._prefix}.STREAM.MSG.GET.{stream_name}"
        resp_data = await self._api_request(
            req_subject, data.encode(), timeout=self._timeout
        )

        raw_msg = api.RawStreamMsg.from_response(resp_data['message'])
        if raw_msg.hdrs:
            hdrs = base64.b64decode(raw_msg.hdrs)
            raw_headers = hdrs[NATS_HDR_LINE_SIZE + _CRLF_LEN_:]
            parsed_headers = self._hdr_parser.parsebytes(raw_headers)
            headers = None
            if len(parsed_headers.items()) > 0:
                headers = {}
                for k, v in parsed_headers.items():
                    headers[k] = v
            raw_msg.headers = headers

        msg_data: Optional[bytes] = None
        if raw_msg.data:
            msg_data = base64.b64decode(raw_msg.data)
        raw_msg.data = msg_data

        return raw_msg

    @classmethod
    def _lift_msg_to_raw_msg(self, msg) -> api.RawStreamMsg:
        if not msg.data:
            msg.data = None
            status = msg.headers.get('Status')
            if status:
                if status == '404':
                    raise NotFoundError
                else:
                    raise APIError.from_msg(msg)

        raw_msg = api.RawStreamMsg()
        subject = msg.headers['Nats-Subject']
        raw_msg.subject = subject

        seq = msg.headers.get('Nats-Sequence')
        if seq:
            raw_msg.seq = int(seq)
        raw_msg.data = msg.data
        raw_msg.headers = msg.headers

        return raw_msg

    async def delete_msg(self, stream_name: str, seq: int) -> bool:
        """
        delete_msg retrieves a message from a stream based on the sequence ID.
        """
        req_subject = f"{self._prefix}.STREAM.MSG.DELETE.{stream_name}"
        req = {'seq': seq}
        data = json.dumps(req)
        resp = await self._api_request(req_subject, data.encode())
        return resp['success']

    async def get_last_msg(
        self,
        stream_name: str,
        subject: str,
        direct: Optional[bool] = False,
    ) -> api.RawStreamMsg:
        """
        get_last_msg retrieves the last message from a stream.
        """
        return await self.get_msg(stream_name, subject=subject, direct=direct)

    async def _api_request(
        self,
        req_subject: str,
        req: bytes = b'',
        timeout: float = 5,
    ) -> Dict[str, Any]:
        try:
            msg = await self._nc.request(req_subject, req, timeout=timeout)
            resp = json.loads(msg.data)
        except NoRespondersError:
            raise ServiceUnavailableError

        # Check for API errors.
        if 'error' in resp:
            raise APIError.from_error(resp['error'])

        return resp


class DirectGetResponse:
    def __init__(
        self,
        manager: JetStreamManager,
        stream: str,
        batch: int,
        seq: int | None = None,
        next_by_subj: str | None = None,
        max_bytes: int | None = None,
        start_time: str | None = None,
        multi_last: list[str] | None = None,
        up_to_seq: int | None = None,
        up_to_time: str | None = None,
        continue_on_eob: bool = False,
    ) -> None:
        self.stream = stream
        self.seq = seq or 1
        self.next_by_subj = next_by_subj
        self.batch = batch
        self.max_bytes = max_bytes
        self.start_time = start_time
        self.multi_last = multi_last
        self.up_to_seq = up_to_seq
        self.up_to_time = up_to_time
        self.continue_on_eob = continue_on_eob
        self._manager = manager
        # Create a new inbox
        self._inbox = manager._nc.new_inbox()
        # Used to continue the batch request
        self._should_continue = False
        # Subscription to the inbox
        self._subscription: Subscription | None = None
        # Status fetched from reply message headers
        self._num_pending: int | None = None
        self._up_to_seq: int | None = None
        self._last_seq: int | None = None
        self._request: dict[str, object] | None = None

    def result(self) -> api.DirectGetResult:
        if (self._up_to_seq is None or self._last_seq is None or self._num_pending is None):
            raise Exception("DirectGetContext not yet completed")
        return api.DirectGetResult(
            num_pending=self._num_pending,
            up_to_seq=self._up_to_seq,
            last_seq=self._last_seq,
        )

    def pending(self) -> bool:
        return self._num_pending is None or self._num_pending > 0

    async def __anext__(self) -> api.RawStreamMsg:
        assert self._subscription is not None
        if self._should_continue:
            await self._fetch_more()
            self._should_continue = False
        msg = await self._subscription.next_msg(timeout=10)
        headers = msg.headers
        assert headers is not None
        status = headers.get("Status")
        if status in ("404", "408", "413"):
            raise APIError.from_msg(msg)
        if status == "204":
            raw_up_to_seq = headers.get("Nats-UpTo-Sequence")
            raw_last_seq = headers.get("Nats-Last-Sequence")
            if not raw_up_to_seq:
                raise Exception("Server does not support batch request")
            if not raw_last_seq:
                raise Exception("Server does not support batch request")
            self._up_to_seq = int(raw_up_to_seq)
            self._last_seq = int(raw_last_seq)
            # Handle continue on EOB
            if self.continue_on_eob and self._num_pending:
                self.__aiter__()
                return await self.__anext__()
            raise StopAsyncIteration
        raw_num_pending = headers.get("Nats-Num-Pending")
        if raw_num_pending:
            self._num_pending = int(raw_num_pending)
        elif self.batch:
            raise Exception("Server does not support batch request")
        headers.pop("Status", None)
        headers.pop("Description", None)
        return self._manager._lift_msg_to_raw_msg(msg)

    def __aiter__(self) -> DirectGetResponse:
        if self._subscription is None:
            raise RuntimeError("DirectGetBatch must be used as an async context manager")
        self._should_continue = True
        if self._request is None:
            req: dict[str, object] = {
                "batch": self.batch,
            }
            if self.seq:
                req["seq"] = self.seq
            if self.next_by_subj:
                req["next_by_subj"] = self.next_by_subj
            if self.max_bytes:
                req["max_bytes"] = self.max_bytes
            if self.start_time:
                req["start_time"] = self.start_time
            if self.multi_last:
                req["multi_last"] = self.multi_last
            if self.up_to_seq:
                req["up_to_seq"] = self.up_to_seq
            if self.up_to_time:
                req["up_to_time"] = self.up_to_time
            self._request = req
        else:
            status = self.result()
            req = {
                "batch": self.batch,
                "seq": status.last_seq + 1,
                "up_to_seq": status.up_to_seq,
            }
            if self.multi_last:
                req["multi_last"] = self.multi_last
            if self.max_bytes:
                req["max_bytes"] = self.max_bytes
            if self.next_by_subj:
                req["next_by_subj"] = self.next_by_subj
            self._request = req
        return self

    async def __aenter__(self) -> DirectGetResponse:
        nc = self._manager._nc
        self._subscription = await nc.subscribe(self._inbox)
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._subscription:
            await self._subscription.unsubscribe()
            self._subscription = None

    async def _fetch_more(self) -> None:
        payload = json.dumps(self._request).encode()
        subject = f"$JS.API.DIRECT.GET.{self.stream}"
        await self._manager._nc.publish(subject, payload, reply=self._inbox)


async def _direct_get(
    m: JetStreamManager,
    stream: str,
    cb: Callable[[api.RawStreamMsg], Awaitable[None]],
    batch: int,
    seq: int | None = None,
    next_by_subj: str | None = None,
    max_bytes: int | None = None,
    start_time: str | None = None,
    multi_last: list[str] | None = None,
    up_to_seq: int | None = None,
    up_to_time: str | None = None,
    continue_on_eob: bool = False,
) -> api.DirectGetResult:
    """Send a direct get request and execute a callback on each message received."""
    nc = m._nc
    inbox = nc.new_inbox()
    _num_pending: int | None = None
    _last_seq: int | None = None
    _up_to_seq: int | None = None
    future: asyncio.Future[api.DirectGetResult] = asyncio.Future()
    request: dict[str, object] = {
        "batch": batch,
    }
    if seq:
        request["seq"] = seq
    if next_by_subj:
        request["next_by_subj"] = next_by_subj
    if max_bytes:
        request["max_bytes"] = max_bytes
    if start_time:
        request["start_time"] = start_time
    if multi_last:
        request["multi_last"] = multi_last
    if up_to_seq:
        request["up_to_seq"] = up_to_seq
    if up_to_time:
        request["up_to_time"] = up_to_time

    async def fetch_more() -> None:
        payload = json.dumps(request).encode()
        subject = f"$JS.API.DIRECT.GET.{stream}"
        await nc.publish(subject, payload, reply=inbox)

    async def raw_cb(msg: Msg) -> None:
        nonlocal _up_to_seq
        nonlocal _last_seq
        nonlocal _num_pending
        nonlocal request
        headers = msg.headers
        assert headers is not None
        status = headers.get("Status")
        if status in ("404", "408", "413"):
            future.set_exception(APIError.from_msg(msg))
            return
        if status == "204":
            raw_up_to_seq = headers.get("Nats-UpTo-Sequence")
            raw_last_seq = headers.get("Nats-Last-Sequence")
            if not raw_up_to_seq:
                future.set_exception(Exception("Server does not support batch request"))
                return
            if not raw_last_seq:
                future.set_exception(Exception("Server does not support batch request"))
                return
            _up_to_seq = int(raw_up_to_seq)
            _last_seq = int(raw_last_seq)
            # Handle continue on EOB
            if continue_on_eob and _num_pending:
                request.clear()
                request["seq"] = _last_seq + 1
                request["up_to_seq"] = _up_to_seq
                request["batch"] = batch
                if multi_last:
                    request["multi_last"] = multi_last
                if max_bytes:
                    request["max_bytes"] = max_bytes
                if next_by_subj:
                    request["next_by_subj"] = next_by_subj
                await fetch_more()
                return
            # We should always have num_pending at this point
            assert _num_pending is not None
            future.set_result(api.DirectGetResult(
                up_to_seq=_up_to_seq,
                last_seq=_last_seq,
                num_pending=_num_pending,
            ))
            return
        raw_num_pending = headers.get("Nats-Num-Pending")
        if raw_num_pending:
            _num_pending = int(raw_num_pending)
        else:
            raise Exception("Server does not support batch request")
        headers.pop("Status", None)
        headers.pop("Description", None)
        raw_msg = m._lift_msg_to_raw_msg(msg)
        await cb(raw_msg)

    # Create the subscription
    sub = await nc.subscribe(inbox, cb=raw_cb)

    # Send a request to fetch more messages
    await fetch_more()

    # Wait until future is set
    try:
        return await future
    # Always unsubscribe
    finally:
        await sub.unsubscribe()
