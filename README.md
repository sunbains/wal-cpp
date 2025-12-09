# WAL Actor Tests

This project contains a WAL implementation with actor-model test drivers. The most useful binary for quick experiments is `build/tests/wal_tests_log_service_actor`.

## Build

```bash
# Configure + build (release flags, warnings as errors)
cmake -S . -B build -G Ninja
ninja -C build
```

For ASAN:

```bash
cmake -S . -B build_asan -G Ninja -DCMAKE_BUILD_TYPE=Debug -DWAL_ENABLE_ASAN=ON
ninja -C build_asan
```

## Running `wal_tests_log_service_actor`

All examples run from repo root. Adjust paths to `build_asan` if you built there.

### Default (1 producer, 1 consumer, 10M messages)
```bash
build/tests/wal_tests_log_service_actor
```

### Disable fdatasync/fsync messages
```bash
build/tests/wal_tests_log_service_actor -f 0
```

### Add periodic fdatasync (10% probability) and batch dequeue
```bash
build/tests/wal_tests_log_service_actor -f 0.1 -b
```

### Disable actual log writes (logic only, no disk IO)
```bash
build/tests/wal_tests_log_service_actor -w
```

### Disable all disk writes (use /dev/null), 4 producers, smaller run
```bash
build/tests/wal_tests_log_service_actor -p 4 -m 1000000 -d
```

### Use fsync instead of fdatasync
```bash
build/tests/wal_tests_log_service_actor -f 0.05 --use-fsync
```

### Verbose metrics (`-v`, add more `v` for histograms)
```bash
build/tests/wal_tests_log_service_actor -f 0.1 -v
# or
build/tests/wal_tests_log_service_actor -f 0.1 -vv
```

### Tuning buffer sizes
```bash
# Smaller buffer (64 blocks) and default block size 4096 bytes
build/tests/wal_tests_log_service_actor --log-buffer-blocks 64

# Larger buffers (2048 blocks) and bigger block size (8192 bytes)
build/tests/wal_tests_log_service_actor --log-buffer-blocks 2048 --log-block-size 8192

# Two I/O threads for background flush/sync
build/tests/wal_tests_log_service_actor --io-threads 2
```

Common flags:
- `-p/--producers NUM`         number of producer actors
- `-m/--messages NUM`          messages per iteration
- `-f/--fdatasync-interval N`  sync probability (0 disables; e.g. 0.1 ≈ 10%)
- `-w/--disable-log-writes`    skip log->append entirely
- `-d/--disable-writes`        write to /dev/null
- `--disable-crc32`            disable CRC32C checksums (default: enabled)
- `-b/--batch-dequeue`         batch dequeue mode
- `-B/--batch-size NUM`        batch size when batch dequeue is enabled
- `-v/--verbose`               increase metrics verbosity (repeatable)
- `--producer-latency`         enable per-message latency metrics (disabled by default for speed)
- `--io-threads NUM`           number of I/O threads for background flush/sync (default: 1)

For full usage, run:
```bash
build/tests/wal_tests_log_service_actor -h
```
