# A Toy Key-Value Store

This key-value store allows us to explore the architectural space for a simple
key-value store, allowing us to tune parameters to suit risk level (for potential
loss of data) and performance.

## File-backed Store

The file-backed store shards values across different files, tunable via the
`--file-count` argument. Note that writing is not designed to be efficient;
it writes the full store each time, so it's important to tune parameters based
on desire to persist data and ability to withstand data loss.

In the future, partial writes using preallocated blocks may be appropriate.
Alternatively, sharding across significantly many files (say ~1000s,
`ulimit`-permitting) would alleviate file backend performance.

The backend supports different `serde` file formats, including JSON and the
binary CBOR format. JSON can allow easier data recovery, but comes at ~2x
performance penalty.

### Synchronous vs Asynchronous File Persisting

We support two styles of file persisting: asynchronous persisting enqueues all
writes into a thread-safe queue, and a background thread pulls items off of the
queue and writes them to disk. If the queue is full, the main thread will block.
Queue depth is set via the `--queue-depth` flag.

Synchronous persisting writes to memory, and periodically flushes to disk based
on the `--write-period-us`. As the name suggests, this write will be blocking.

There's an opportunity to periodically flush in the asynchronous backend; that
is work that we can explore later.

## Example Invocation

To run the load test using the default "consistent" pattern and JSON serializer
on a machine with 4 (hyperthreaded) processors.

```
cargo run --release -- \
    --threads=4 \
    --load-time-sec=10 \
    file --file-count=120 --queue-depth 1024 --serializer=json
```

## Design Space

The design space is significant, and will vary based on hardware (SSDs, CPU, etc.).

To give a sense of how file-based backends perform, it's helpful to run a memory-only baseline:

```
cargo run --release -- \
    --threads=4 \
    --load-time-sec=10 \
    memory
```

On a 2015 Macbook, this produces:
```
total_ops: 1708513
total_runtime: 10.000090816s
total_ops_per_sec: 170849.75
average_ops_per_sec: 42712.69
```

We can then determine, say, that we are tolerant to a few milliseconds of data loss
due to application-level robustness. So we may consider the asynchronous writer with
a queue depth of 8192, sharding the files across 128 files (default ulimit for MacOS):
```
cargo run --release -- \
    --threads=4 \
    --load-time-sec=10 \
    file --file-count=128 --queue-depth=8192
```

We see that we run ~4% of the total throughput, which is not surprising given we
added disk I/O in the loop. However, performance is still reasonable at roughly
1800 operations per second per thread.
```
total_ops: 75983
total_runtime: 10.03314522s
total_ops_per_sec: 7573.20
average_ops_per_sec: 1893.46
```

Changing the backend from JSON to CBOR does not improve the runtime, suggesting
that we're sufficiently latency-hiding.
```
cargo run --release -- \
    --threads=4 \
    --load-time-sec=10 \
    file --file-count=128 --queue-depth=8192 --serializer=cbor
```

Resulting in roughly the same performance:
```
total_ops: 79581
total_runtime: 10.085022077s
total_ops_per_sec: 7891.01
average_ops_per_sec: 1972.77
```
