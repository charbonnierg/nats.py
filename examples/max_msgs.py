import asyncio
from collections import deque
from typing import Deque
from nats import NATS
from nats.aio.msg import Msg

RECEIVED: Deque[Msg] = deque()


async def cb(msg: Msg) -> None:
    global RECEIVED
    RECEIVED.append(msg)


nc = NATS()


async def main():
    global RECEIVED

    await nc.connect()

    await nc.subscribe("foo", cb=cb, max_msgs=5)

    for _ in range(20):
        await nc.publish("foo")

    await asyncio.sleep(0.1)

    print(len(RECEIVED))
    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
