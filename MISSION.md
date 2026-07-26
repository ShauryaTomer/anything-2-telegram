# Mission: Understand pyee Before Building

## Why
Build and safely change an event-driven Python server that downloads artifacts and uploads them to Telegram without hiding runtime behavior behind a library.

## Success looks like
- Explain Observer and publish-subscribe using this server's runtime flow
- Read pyee subscription and emission code without guessing
- Predict sync, async, failure, process, and lifecycle behavior
- Review future event-bus implementation decisions confidently

## Constraints
- Existing backend, networking, and HTTP knowledge
- Mermaid and concise written explanations preferred
- pyee `AsyncIOEventEmitter` selected for the first single-process version

## Out of scope
- Distributed brokers and delivery guarantees
- General design-pattern survey
- Production implementation before architecture approval
