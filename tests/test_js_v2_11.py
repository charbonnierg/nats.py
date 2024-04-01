import asyncio
from contextlib import ExitStack
import pytest
import shutil

from nats import NATS
from nats.js import api

from tests.utils import NATSD, start_natsd, async_test


class SingleJetStreamServerTestCase:

    @pytest.fixture(autouse=True)
    def setup(self):
        with ExitStack() as stack:
            natsd = NATSD(port=4222, with_jetstream=True) 
            self.loop = loop = asyncio.new_event_loop()
            self.server_pool = [natsd]
            stack.callback(loop.close)
            stack.callback(lambda: shutil.rmtree(natsd.store_dir) if natsd.store_dir else None)
            stack.callback(natsd.stop)
            start_natsd(natsd)
            yield


class TestDirectGetBatch(SingleJetStreamServerTestCase):

    @async_test
    @pytest.mark.parametrize(
        "batch_size, seq, multi_last, num_pending, last_seq, up_to_seq",
        [
            (10, 1, None, 90, 10, None),
            (10, 1, ["test.*"], 90, 10, 100),
            (100, 1, None, 0, 100, None),
            (100, 1, ["test.*"], 0, 100, 100),
        ]
    )
    async def test_something(
        self,
        batch_size: int,
        seq: int,
        multi_last: list[str] | None,
        num_pending: int,
        last_seq: int,
        up_to_seq: int | None,
    ):
        nc = NATS()
        await nc.connect()
        js = nc.jetstream()
        await js.add_stream(
            api.StreamConfig(name="m1", subjects=["test.*"], allow_direct=True)
        )
        for i in range(100):
            await nc.request(f"test.{i}", f"{i}".encode())

        current = 0
        async def cb(msg: api.RawStreamMsg) -> None:
            nonlocal current
            assert msg.subject == f"test.{current}"
            assert msg.data == f"{current}".encode()
            assert msg.sequence == current + 1

            current += 1

        result = await js.direct_get(
            "m1",
            cb=cb,
            batch_size=batch_size,
            seq=seq,
            multi_last=multi_last,
        )
        assert current == batch_size
        assert result.num_pending == num_pending
        assert result.last_seq == last_seq
        assert result.up_to_seq == up_to_seq
