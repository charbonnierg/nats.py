import asyncio
import logging
import sys
import random

from nats import connect, NATS
from nats.aio.client import DEFAULT_PENDING_SIZE

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
    # CASE 1: OK
    for i in range(100):
        await asyncio.create_task(publisher(nc))
    # CASE 2: OK
    await asyncio.gather(*[publisher(nc) for i in range(100)])
    # CASE 3: OK
    for i in range(100):
        asyncio.ensure_future(publisher(nc))
    # CASE 4: OK
    for i in range(100):
        await publisher(nc)
    # Sleep for some time
    await asyncio.sleep(5)    

async def main():
    # Define max pending data size
    MAX_PENDING_DATA = DEFAULT_PENDING_SIZE
    # Connect to NATS
    nc = await connect(pending_size=MAX_PENDING_DATA)
    # Start publishing
    await start_publish(nc)
    # Drain the connection
    await nc.drain()
    # Display stats
    logging.warning(nc.stats)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
