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
