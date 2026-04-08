# goreplay-grpc-parser

A generic gRPC traffic parser. Reads raw [goreplay](https://github.com/buger/gor) TCP captures from a Kafka topic or file, decodes the gRPC requests using local proto files, and publishes parsed JSON to a destination Kafka topic or file.

Written with help of Claude.

## Overview

```
goreplay sidecar (captures TCP traffic)
         │
         ▼
Source (Kafka topic or file)   ← raw binary, goreplay format
         │
         ▼
goreplay-grpc-parser   (this tool)
         │
         ▼
Destination (Kafka topic or file)   ← decoded JSON
```

---

## Quick Start

### Prerequisites

- Go 1.25+ (or download a pre-built binary from [Releases](https://github.com/sumanthkumarc/goreplay-grpc-parser/releases))
- Proto files for your gRPC service
- Well-known proto files (`google/api/`, `google/protobuf/`, `validate/`) — auto-downloaded if not provided

### Capture traffic with goreplay

```bash
./gor \
  --input-raw :6060 \
  --input-raw-protocol binary \
  --output-kafka-host "kafka-broker:9092" \
  --output-kafka-topic "my-service-grpc-raw"
```

> **Important:** The source Kafka topic must be a **single-partition** topic. Ordering of TCP packets within a connection must be preserved for correct HTTP/2 reassembly.

### Build

```bash
git clone https://github.com/sumanthkumarc/goreplay-grpc-parser
cd goreplay-grpc-parser
go build -o parser .
```

### Run

**Kafka → Kafka:**
```bash
./parser \
  --source-brokers="kafka-broker:9092" \
  --source-topic="my-service-grpc-raw" \
  --dest-brokers="kafka-broker:9092" \
  --dest-topic="my-service-grpc-parsed" \
  --proto-path="./proto" \
  --consumer-group="my-parser-group" \
  --workers=8
```

**Kafka → stdout (quick test):**
```bash
./parser \
  --source-brokers="kafka-broker:9092" \
  --source-topic="my-service-grpc-raw" \
  --dest-file=- \
  --proto-path="./proto" \
  --consumer-group="my-parser-group"
```

**File → file (no Kafka required):**
```bash
./parser \
  --source-file="./dump.bin" \
  --dest-file="./output.jsonl" \
  --proto-path="./proto"
```

If `--well-known-path` is not set or the path doesn't exist, well-known protos are automatically downloaded to `/tmp/well-known-protos/` at startup.

### CLI Flags

**Source (mutually exclusive):**

| Flag | Default | Description |
|------|---------|-------------|
| `--source-brokers` | — | CSV list of Kafka broker addresses for the input topic |
| `--source-topic` | — | Input Kafka topic containing raw goreplay binary data |
| `--source-file` | — | Path to a goreplay dump file (alternative to Kafka source) |

**Destination (mutually exclusive):**

| Flag | Default | Description |
|------|---------|-------------|
| `--dest-brokers` | — | CSV list of Kafka broker addresses for the output topic |
| `--dest-topic` | — | Output Kafka topic for parsed JSON messages |
| `--dest-file` | — | Path to output file for JSON lines; use `-` for stdout (alternative to Kafka dest) |

**Proto:**

| Flag | Default | Description |
|------|---------|-------------|
| `--proto-path` | — | **required** — Path to `proto/` directory |
| `--well-known-path` | auto-download | Path to well-known protos dir (`google/`, `validate/`); downloads to `/tmp/well-known-protos` if absent |

**Tuning:**

| Flag | Default | Description |
|------|---------|-------------|
| `--consumer-group` | — | **required when using Kafka source** — Kafka consumer group ID |
| `--workers` | `4` | Number of parallel message processing goroutines |
| `--idle-timeout` | `10s` | Exit after this long with no new messages (0 = run forever) |
| `--offset` | `-1` | Start from a specific partition offset, bypassing committed group offsets |
| `--debug` | `false` | Read from beginning, no output written, print frame-level diagnostic stats on exit |

---

## Output Format

Each message published to the destination is a JSON object:

```json
{
  "timestamp": 1774937831561622325,
  "method": "mypackage.MyService/MyMethod",
  "headers": {
    "user-id": "12345",
    "x-trace-id": "abc-def-ghi"
  },
  "request": {
    "field1": "value1",
    "nested": {
      "field2": 42
    }
  }
}
```

- `timestamp` — nanoseconds from goreplay capture header
- `method` — gRPC path with leading `/` stripped (`Package.Service/Method` format)
- `headers` — all HTTP/2 request headers, excluding pseudo-headers (`:method`, `:path`, `:scheme`, `:authority`)
- `request` — the decoded protobuf request body as JSON

---

## Debug Mode

The `--debug` flag is useful for verifying your setup without writing any output:

```bash
./parser \
  --source-brokers="kafka-broker:9092" \
  --source-topic="my-service-grpc-raw" \
  --dest-file=- \
  --proto-path="./proto" \
  --consumer-group="my-parser-debug" \
  --debug
```

In debug mode the parser:
- Reads from the **beginning** of the topic (bypasses committed group offsets)
- Does **not** write anything to the destination
- Prints a per-method breakdown and frame-level diagnostic stats on exit

Example output:

```
=== Final Stats ===
Processed: 7770  Skipped: 0  Total: 7770
--- Per-method breakdown ---
  1899  mypackage.MyService/MethodA
  1247  mypackage.MyService/MethodB
=== Debug: Frame-level Stats ===
  Raw Kafka messages:          15748
  HEADERS frames:              7843
  DATA frames:                 7843
  DATA emitted:                7770
  DATA empty body:             73
  DATA decompressed OK:        7707
  DATA unknown compression:    0
```

---

## How It Works

### goreplay captures raw TCP packets

goreplay sniffs traffic at the kernel level and pushes each TCP segment to Kafka. Each Kafka message is a plain-text header line followed by the raw binary payload:

```
1 5b8817ac0a09ad5b 1775544696692543319 0\n
<raw binary bytes...>
```

Fields: `type id timestamp_ns latency`. Type `1` = request.

### TCP reassembly

Because gRPC runs over HTTP/2 and each gRPC call can span multiple TCP segments, the parser maintains per-connection state keyed by the goreplay connection ID (`id` field). TCP payload bytes are buffered per connection until a complete HTTP/2 frame is available.

### HTTP/2 frame parsing

HTTP/2 frames have a 9-byte header: 3-byte length, 1-byte type, 1-byte flags, 4-byte stream ID. The parser processes two frame types:

| Frame type | Contains |
|------------|----------|
| `HEADERS` (`0x1`) | HTTP headers decoded via HPACK: `:path`, `content-type`, custom metadata |
| `DATA` (`0x0`) | The protobuf-encoded request body with a 5-byte gRPC envelope |

HPACK state is maintained per connection (connection-scoped, not stream-scoped), so indexed header references are resolved correctly across packets.

### gRPC message envelope

Inside each DATA frame, gRPC wraps the protobuf bytes:

```
compressed (1 byte) | message_length (4 bytes big-endian) | protobuf bytes
```

If `compressed = 1`, the codec is auto-detected from the payload's magic bytes. Currently supported: Snappy framing format (`grpc-encoding: snappy`). If no registered codec matches, the message is counted as `DATA unknown compression` in debug stats and skipped.

### Pluggable compression

The `internal/compression` package defines a `Decompressor` interface. Each codec lives in its own file and declares its magic byte prefix for auto-detection:

```go
type Decompressor interface {
    Magic() []byte
    Decompress(src []byte) ([]byte, error)
}
```

To add a new codec, implement this interface and register it with `compression.NewRegistry(...)`.

### Dynamic protobuf decoding

Proto files are parsed at runtime using [grpcurl](https://github.com/fullstorydev/grpcurl) and [jhump/protoreflect](https://github.com/jhump/protoreflect) — no code generation needed. This makes the parser generic across any gRPC service.

---

## Project Structure

```
goreplay-grpc-parser/
├── main.go                        # CLI wiring, worker pool, stats
├── internal/
│   ├── compression/
│   │   ├── compression.go         # Decompressor interface + Registry
│   │   └── snappy.go              # Snappy framing codec
│   ├── decoder/
│   │   └── decoder.go             # Dynamic protobuf decoding
│   ├── input/
│   │   ├── input.go               # Reader interface
│   │   ├── kafka.go               # Kafka source
│   │   └── file.go                # File source
│   ├── output/
│   │   ├── output.go              # Writer interface
│   │   ├── kafka.go               # Kafka destination
│   │   └── file.go                # File/stdout destination
│   ├── tracker/
│   │   └── tracker.go             # TCP reassembly + HTTP/2 frame parsing
│   └── wellknown/
│       └── wellknown.go           # Well-known proto auto-download
├── .goreleaser.yaml
├── .github/workflows/release.yml  # Release workflow
├── go.mod
├── go.sum
└── LICENSE
```

---

## Releases

Pre-built binaries for Linux (amd64) and macOS (arm64) are available on the [Releases](https://github.com/sumanthkumarc/goreplay-grpc-parser/releases) page, built automatically on each version tag.

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
