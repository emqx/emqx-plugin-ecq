# ECQ - Exclusive Compacted Queue

Exclusive compacted queue of EMQX.

This plugin makes use of conventional topic patterns to implement exclusive compacted queues for MQTT subscribers.

- **Producer**: Publishes messages to a topic of the form `$ECQ/w/{CLIENTID}/{key}`.
- **Consumer**: Subscribes must to a topic of the form `$ECQ/{CLIENTID}/#` with QoS=1.

The `key` is the topic suffix, it is used to identify the message for compaction.

The `w` prefix is added to unpair the PUBLISH and SUBSCRIBE topics so the messages are not accidentally delivered to the consumer per regular MQTT message routing.

## Exclusive vs Shared Consumption

This plugin implements exclusive consumption, meaning each queue can only have one active consumer at a time. This is optimized for high write rates where messages need to be processed by exactly one consumer.

For shared consumption where multiple consumers can process messages from the same queue, ECS (Exclusive Compacted Shared) plugin will be released instead. ECS focuses on lower write rates, plus heavy fanout reads (the same message for many consumers).

## How It Differs from Retained Messages

ECQ is similar to retained messages. Below features are the main differences:

- ECQ is a real 'queue', EMQX will record message sequence number for each subscriber to avoid re-delivering messages to the consumer after it reconnect/resubscribe.
- Subscribers can rewind to any sequence number (TODO).

## Quick Start

- Start EMQX cluster with plugin installed: `make run`
- Mock consumer to subscribe ECQ topic: `./scripts/sub.sh 1`
- Mock producer to publish messages to ECQ topic: `./scripts/send.sh`

## Data Storage

Internally, data is stored in below mnesia (rocksdb) tables:

- `ecq_seqno`: Stores the sequence number assignment state for each `CLIENTID`.
- `ecq_state`: Stores the consumer state for each `CLIENTID` (the last sequence number that has been acknowledged by the consumer, and the last timestamp of the acknowledgement).
- `ecq_index`: Map sequence number to the message key. Key is the composite of `CLIENTID` and `seqno`, value is the `key` part of the topic, and the `timestamp` of the message (when it is published).
- `ecq_payload`: Stores the messages. Key is the composite of `CLIENTID`, `key`, `seqno` and `timestamp`, value is the MQTT message payload.

The tables reside only in 'core' nodes in the cluster.
The 'replicant' nodes perform PRC calls for writes and reads.

### Compaction

When a new `key` is published, the new message is assigned with the next sequence number, then inserted into the `ecq_payload` table, then the old message with the same `key` is deleted from both `ecq_index` and `ecq_payload`.

### Garbage Collection

This plugin runs periodic garbage collection to delete from `ecq_index` and `ecq_payload` if the timestamp is older than the configured retention period.

## Data Flow

For producers, the message PUBLISH is essentially a write operation to the tables.
For consumers, there are two types of messaging flow:

- Realtime Forwarding: when the consumer is online, the messages are stored in the tables, and sent to the consumer immediately.
- Reactive Forwarding: when the consumer is offline, the messages are stored for later delivery when the consumer comes back online and subscribes to the `$ECQ/{CLIENTID}/#` topic.

To ensure message ordering, for each `CLIENTID`, all writes will have to be done by a deterministic process running on the core node. There is a pool of agents running on all core nodes to handle below events:

- Append: A new message is published to a queue, append it and maybe send it to the consumer immediately.
- Acked: A message is acknowledged by the consumer, update the consumer state and send the next message(s) to the consumer.
- Subscribed: After a consumer subscribes to the `$ECQ/{CLIENTID}/#` topic, start sending messages to the consumer.
- Heartbeat: Optional, this works as a fallback mechanism to ensure messages are eventually sent to the consumer even if a subscribed event was previously missed.

## Security Considerations

Topics matching `$ECQ/{CLIENTID}/#` should only be allowed by the consumer. ACL rules should be added to ensure this.

For example, in `acl.conf`:
```
{allow, {clientid, {re, "^ecq-.+"}}, subscribe, ["eq $ECQ/${clientid}/#"]}.
```
