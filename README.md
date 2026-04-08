# goreplay-grpc-parser

A generic gRPC traffic parser. Reads raw [goreplay](https://github.com/buger/gor) TCP captures from a Kafka topic, decodes the gRPC requests using local proto files, and publishes parsed JSON to a destination Kafka topic.

## Overview

```
goreplay sidecar (captures TCP traffic)
         │
         ▼
Source Kafka Topic  (raw binary, goreplay format)
         │
         ▼
goreplay-grpc-parser   (this tool)
         │
         ▼
Destination Kafka Topic  (decoded JSON)
```

---

## Quick Start

### Prerequisites

- Go 1.21+
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
go build ./cmd/parser/
```

### Run

```bash
./parser \
  --source-brokers="kafka-broker:9092" \
  --source-topic="my-service-grpc-raw" \
  --dest-brokers="kafka-broker:9092" \
  --dest-topic="my-service-grpc-parsed" \
  --proto-path="./proto" \
  --workers=8
```

If `--well-known-path` is not set or the path doesn't exist, well-known protos are automatically downloaded to `/tmp/well-known-protos/` at startup.

### CLI Flags

| Flag | Default | Required | Description |
|------|---------|----------|-------------|
| `--source-brokers` | — | yes | CSV list of Kafka broker addresses for the input topic |
| `--source-topic` | — | yes | Input Kafka topic containing raw goreplay binary data |
| `--dest-brokers` | — | yes | CSV list of Kafka broker addresses for the output topic |
| `--dest-topic` | — | yes | Output Kafka topic for parsed JSON messages |
| `--proto-path` | — | yes | Path to `proto/` directory |
| `--well-known-path` | auto-download | no | Path to well-known protos dir (`google/`, `validate/`); downloads to `/tmp/well-known-protos` if absent |
| `--consumer-group` | `goreplay-grpc-parser` | no | Kafka consumer group ID |
| `--workers` | `4` | no | Number of parallel message processing goroutines |
| `--idle-timeout` | `10s` | no | Exit after this long with no new messages (0 = run forever) |
| `--offset` | `-1` | no | Start from a specific partition offset, bypassing committed group offsets |
| `--debug` | `false` | no | Read from beginning, no Kafka output, print frame-level diagnostic stats on exit |

---

## Output Format

Each message published to the destination topic is a JSON object:

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

The `--debug` flag is useful for verifying your setup before writing to Kafka:

```bash
./parser \
  --source-brokers="kafka-broker:9092" \
  --source-topic="my-service-grpc-raw" \
  --dest-brokers="kafka-broker:9092" \
  --dest-topic="my-service-grpc-parsed" \
  --proto-path="./proto" \
  --debug
```

In debug mode the parser:
- Reads from the **beginning** of the topic (bypasses committed group offsets)
- Does **not** write anything to the destination topic
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
  DATA snappy OK:              7707
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

If `compressed = 1`, the body is decompressed using the Snappy framing format before protobuf decoding. If a different encoding is encountered, the message is counted as `DATA unknown compression` in debug stats and skipped.

### Dynamic protobuf decoding

Proto files are parsed at runtime using [grpcurl](https://github.com/fullstorydev/grpcurl) and [jhump/protoreflect](https://github.com/jhump/protoreflect) — no code generation needed. This makes the parser generic across any gRPC service.

---

## Project Structure

```
goreplay-grpc-parser/
├── cmd/
│   └── parser/
│       └── main.go      # Parser service
├── go.mod
├── go.sum
├── LICENSE
└── README.md
```

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
