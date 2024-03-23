import asyncio
import nats
from nats.js import api
from nats.js import errors


async def main():
    nc = await nats.connect("localhost")

    # Create JetStream context.
    js = nc.jetstream()

    # Clean-up KV in case it already exists
    try:
        await js.delete_key_value("demo")
    except errors.NotFoundError:
        pass

    # Create a KV store
    kv = await js.create_key_value(
        api.KeyValueConfig(bucket="demo", history=64, direct=True)
    )

    # Set a key
    await kv.put("device.a", b"bar")

    # Set another key
    await kv.put("device.b", b"foo")

    # Update a key
    await kv.put("device.a", b"baz")

    # Set a key
    await kv.put("device.c", b"qux")

    # Use case: Read latest value for all devices with a batch size

    # Usage 1: Async iterator which can be reentered
    async with js.direct_get(
        "KV_demo",
        batch_size=10,
        multi_last=["$KV.demo.device.*"],
    ) as response:

        # Because a batch size is used,
        # we need to loop until response
        # is no longer pending.
        while response.pending():

            # Iterate over a batch of messages
            async for msg in response:
                print(f"Pending: {response.pending()}")
                print(f"Data: {msg.data}")

            # Print response result. It's only possible
            # to access result when a batch has been fully
            # processed.
            result = response.result()
            print(f"Result: {result}")

    # Usage 2: Async iterator which does not need to be reentered
    async with js.direct_get(
        "KV_demo",
        batch_size=1,
        multi_last=["$KV.demo.device.*"],
        continue_on_eob=True,
    ) as response:

        async for msg in response:
            print(f"Pending: {response.pending()}")
            print(f"Data: {msg.data}")

    result = response.result()
    print(f"Result: {result}")

    # Usage 3: Using a callback to process messages until batch is ended

    async def cb(msg: api.RawStreamMsg) -> None:
        print(f"Msg sequence: {msg.seq}")
        print(f"Data: {msg.data}")

    # Read latest value using a callback
    result = await js.direct_get(
        "KV_demo",
        batch_size=1,
        cb=cb,
        multi_last=["$KV.demo.device.*"],
    )
    print(result)
    # Continue reading from last sequence
    while result.num_pending:
        result = await js.direct_get(
            "KV_demo",
            batch_size=1,
            cb=cb,
            multi_last=["$KV.demo.device.*"],
            continue_from=result,
        )

    # Usage 4: Using a callback to process messages until whole response is received
    result = await js.direct_get(
        "KV_demo",
        batch_size=10,
        cb=cb,
        multi_last=["$KV.demo.device.*"],
        continue_on_eob=True,
    )

if __name__ == "__main__":
    asyncio.run(main())
