# Test Plan

Basic tunning:

On top of the default tunnings for performance tests, fo this specific test, we need to:

- Set `dump_log_write_threshold` to 20000 in vm.args.

## Write Performance

**Objective**:

- Find the smallest interval for publishers to publish without causing:
 - `Mnesia overload` warning.
 - `failed_to_append_message` error log (e.g. `{erpc, timeout}`).
- Observe the output of `emqx ctl ecq status` output to estimate the average memory and disk usage for each consumer.

**Cluster**:

- 3 core nodes
- 0 replicant nodes

**Producer**:

- Start 1M emqtt-bench clients to publish to 1M topics with topic pattern `$ECQ/w/%i/%rand_1000`.
- Payload size: 1KB

## Read Performance

**Objective**:

- Find the maximum throughput of outgoing messages before network or the nodes are overloaded.
- Observe the output of `emqx ctl ecq status` output to estimate the average memory and disk usage for each consumer.

**Cluster**:

- 3 core nodes
- 0 replicant nodes

**Subscribes**

- Start 1M emqtt-bench clients to subscribe to 1M topics with topic pattern `$ECQ/%i/#`.
- Each client must use %i for its client ID.

## Mixed Traffic

NOTE: need to be more specific about the traffic.

**Objective**:

- Find the minimum cluster size to support 20M clients running 1-to-1 PUB/SUB.

**Traffic**:

- 5M publishers publishing.
  - 0.5M clients (10% of total) publishing to topic `$ECQ/%rand_10000000/%rand_10`.
  - 4.5M clients (90% of total) publishing to topic `normal/%rand_10000000/%rand_10`.
- 5M subscribers subscribing.
  - All clients subscribing to both `$ECQ/%i/#` and `normal/%i/#`.
