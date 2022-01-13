import asyncio
import logging
import sys
import random
from timeit import default_timer

from nats import connect, NATS
import nats.aio.client

root = logging.getLogger()

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
handler.setFormatter(formatter)

loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
for logger in loggers:
    print(f"Found logger: {logger.name}")
    logger.setLevel(logging.DEBUG)
root.addHandler(handler)


async def publisher(nc: NATS):
    # Let's publish 1000 times in a row
    for i in range(1000):
        data = b"A" * random.randint(1000, 100_000)
        await nc.publish("foo", data)
        if nc.stats["out_bytes"] == 1_000_000_000:
            logger.warning("Sent 1GB")
        if nc.stats["out_bytes"] == 2_000_000_000:
            logger.warning("Sent 2GB")
        if nc.stats["out_bytes"] == 3_000_000_000:
            logger.warning("Sent 3GB")
        if nc.stats["out_bytes"] == 4_000_000_000:
            logger.warning("Sent 4GB")


async def start_publish(nc: NATS):
    # CASE 1: Using asyncio.gather
    await asyncio.gather(*[publisher(nc) for i in range(10)])
    # CASE 2 Using a for loop
    for i in range(100):
        await publisher(nc)


async def main(pending_size: int):
    # Override default pending size
    nats.aio.client.DEFAULT_PENDING_SIZE = pending_size
    # Connect to NATS
    nc = await connect()
    start = default_timer()
    # Start publishing
    await start_publish(nc)
    # Drain the connection
    await nc.drain()
    end = default_timer()
    # Display stats
    logging.warning(nc.stats)
    logger.warning(f"Duration: {(end - start):3f} seconds")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main(1024*1024))
