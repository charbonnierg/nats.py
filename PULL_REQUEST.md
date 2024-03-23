## Problem

## Reference

- [jsm.go #530 - Support direct batch and multi get](https://github.com/nats-io/jsm.go/pull/530/files)
- [jsm.go implementation](https://github.com/nats-io/jsm.go/blob/1c7f0e42497859a4594efbc657ca25237cab7359/streams.go#L749)
- [StreamMsgGetRequest JSON Schema](https://github.com/nats-io/jsm.go/blob/main/schemas/jetstream/api/v1/stream_msg_get_request.json)

### Edge cases


- When requests are made against servers that do not support `batch` the first response will be received and nothing will follow. There is no way to detect this scenario and the batch request will time out. Language documentation must clearly call out what server version supports this.

- To help inform proper use of this feature vs just using a consumer, any multi-subject request may only allow matching up to 1024 subjects. Any more will result in a 413 status reply.
