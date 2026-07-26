# pyee, asyncio, and Observer Resources

## Knowledge

- [PyPubSub project documentation](https://pypubsub.readthedocs.io/en/v4.0.3/)
  Primary source for single-process Observer semantics, synchronous delivery, topics, listeners, and message data.
- [PyPubSub basic tasks](https://pypubsub.readthedocs.io/en/v4.0.3/usage/usage_basic_tasks.html)
  Primary source for subscription, `sendMessage`, topic hierarchy, listener ordering, and Message Data Specification behavior.
- [PyPubSub public API](https://pypubsub.readthedocs.io/en/v4.0.3/usage/module_pub.html)
  Primary source for `subscribe`, `unsubscribe`, publishers, listener validation, and topic APIs.
- [Python `asyncio` coroutines and tasks](https://docs.python.org/3/library/asyncio-task.html)
  Primary source for coroutine creation, `await`, `create_task`, task ownership, and `asyncio.run` constraints.
- [Python `asyncio` queues](https://docs.python.org/3/library/asyncio-queue.html)
  Primary source for `maxsize` backpressure, `put`/`get` blocking semantics, and producer/consumer task pairs. Basis for bounded-prefetch designs.
- [Python `asyncio` synchronization primitives](https://docs.python.org/3/library/asyncio-sync.html)
  Primary source for `Lock` semantics and FIFO waiter ordering when serializing coroutines.
- [pyee documentation](https://pyee.readthedocs.io/en/latest/)
  Primary source for pyee's EventEmitter model and supported async implementations.
- [pyee API: `AsyncIOEventEmitter`](https://pyee.readthedocs.io/en/latest/api/#pyee.asyncio.AsyncIOEventEmitter)
  Primary source for async-handler scheduling, `error` events, pending-handler tracking, graceful waiting, and cancellation.
- [pyee changelog](https://pyee.readthedocs.io/en/latest/changelog/)
  Primary source for current import paths and lifecycle API additions.
- [Enterprise Integration Patterns: Asynchronous Request-Response](https://www.enterpriseintegrationpatterns.com/patterns/conversation/RequestResponse.html)
  Primary pattern reference for request/reply messaging and its correlation requirements.
- [Enterprise Integration Patterns: Command Message](https://www.enterpriseintegrationpatterns.com/patterns/messaging/CommandMessage.html)
  Primary reference for messages that instruct a named consumer to act. Use to classify `*_REQUESTED` events.
- [Enterprise Integration Patterns: Event Message](https://www.enterpriseintegrationpatterns.com/patterns/messaging/EventMessage.html)
  Primary reference for messages that report a fact to zero-to-many observers. Use to classify `*_READY` / `*_UPLOADED` events.
- [Azure Architecture Center: Asynchronous Request-Reply](https://learn.microsoft.com/en-us/azure/architecture/patterns/asynchronous-request-reply)
  Trusted reference for when long-running work justifies splitting acceptance from completion, including HTTP 202 and job-status resources.
- Gamma, Helm, Johnson, Vlissides, _Design Patterns: Elements of Reusable Object-Oriented Software_, Observer chapter.
  Original catalog definition of Observer. Use for pattern roles and consequences; PyPubSub docs remain authoritative for library behavior.

## Wisdom (Communities)

- [pyee GitHub issues](https://github.com/jfhbrook/pyee/issues)
  Maintainer/user evidence for library-specific edge cases, bugs, and current behavior.
