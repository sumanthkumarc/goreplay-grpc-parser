// Package tracker reassembles goreplay TCP captures into complete gRPC requests.
//
// goreplay emits one Kafka message per TCP segment. This package buffers bytes
// per-connection, parses HTTP/2 frames (HEADERS + DATA), performs HPACK
// decoding, unwraps the gRPC envelope, and emits a CompleteRequest once a
// request path and non-empty body are available.
package tracker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/sumanthkumarc/goreplay-grpc-parser/internal/compression"
	"golang.org/x/net/http2/hpack"
)

const (
	gorHeaderTypeRequest = "1"
	http2Preface         = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
)

// CompleteRequest is a fully reassembled gRPC request ready for proto decoding.
type CompleteRequest struct {
	Timestamp int64
	Path      string
	Headers   map[string]string
	Body      []byte
}

// Stats holds frame-level counters exposed for debug output.
type Stats struct {
	RawMessages            int64
	NonRequest             int64 // goreplay type != "1"
	FramesHeaders          int64
	FramesData             int64
	FramesOther            int64
	DataNoPath             int64 // DATA arrived but no :path known yet
	DataEmitted            int64
	DataEmptyBody          int64 // DATA frame with msgLen==0
	DataAlreadyEmit        int64 // DATA frame but conn.emitted already true
	DataTooShort           int64 // DATA payload < 5 bytes (no gRPC envelope)
	DataMsgLenMismatch     int64 // gRPC msgLen > available body bytes
	DataDecompressOK       int64 // compressed flag set, decompression succeeded
	DataUnknownCompression int64 // compressed flag set, no matching codec
}

// connState holds per-TCP-connection reassembly state.
type connState struct {
	hpackDec *hpack.Decoder
	rawBuf   []byte            // unprocessed bytes waiting for a complete HTTP/2 frame
	headers  map[string]string // decoded from the most recent HEADERS frame
	path     string
	bodyBuf  []byte
	emitted  bool
}

// ConnTracker reassembles goreplay TCP packets into complete gRPC requests.
// It must be driven from a single goroutine (no internal locking).
type ConnTracker struct {
	conns    map[string]*connState
	debug    bool
	stats    Stats
	registry *compression.Registry
}

// New returns a ConnTracker. Pass a non-nil registry to enable decompression;
// if registry is nil, compression.Default() is used.
func New(debug bool, registry *compression.Registry) *ConnTracker {
	if registry == nil {
		registry = compression.Default()
	}
	return &ConnTracker{
		conns:    make(map[string]*connState),
		debug:    debug,
		registry: registry,
	}
}

// Stats returns a copy of the current frame-level counters.
func (ct *ConnTracker) Stats() Stats { return ct.stats }

// Feed processes one raw goreplay Kafka message and returns a CompleteRequest
// when a full gRPC request is available, or nil if more packets are needed.
func (ct *ConnTracker) Feed(data []byte) *CompleteRequest {
	ct.stats.RawMessages++

	headerBytes, payload, ok := bytes.Cut(data, []byte("\n"))
	if !ok {
		return nil
	}

	parts := strings.Fields(string(headerBytes))
	if len(parts) < 2 {
		return nil
	}
	if parts[0] != gorHeaderTypeRequest {
		ct.stats.NonRequest++
		return nil
	}

	connID := parts[1]

	var timestamp int64
	if len(parts) >= 3 {
		fmt.Sscanf(parts[2], "%d", &timestamp)
	}

	if bytes.HasPrefix(payload, []byte(http2Preface)) {
		payload = payload[len(http2Preface):]
	}

	conn := ct.getOrCreate(connID)
	conn.rawBuf = append(conn.rawBuf, payload...)
	return ct.processFrames(conn, timestamp)
}

func (ct *ConnTracker) getOrCreate(connID string) *connState {
	if conn, ok := ct.conns[connID]; ok {
		return conn
	}
	dec := hpack.NewDecoder(65536, nil)
	dec.SetMaxStringLength(1 << 20) // 1 MiB — accommodates large header values
	conn := &connState{
		hpackDec: dec,
		headers:  make(map[string]string),
	}
	ct.conns[connID] = conn
	return conn
}

func (ct *ConnTracker) processFrames(conn *connState, timestamp int64) *CompleteRequest {
	var result *CompleteRequest
	data := conn.rawBuf
	offset := 0

	for offset+9 <= len(data) {
		length := int(data[offset])<<16 | int(data[offset+1])<<8 | int(data[offset+2])
		frameType := data[offset+3]
		flags := data[offset+4]

		if offset+9+length > len(data) {
			break // incomplete frame — wait for next packet
		}
		framePayload := data[offset+9 : offset+9+length]

		switch frameType {
		case 0x1: // HEADERS
			ct.stats.FramesHeaders++
			// Flush any pending un-emitted body before resetting stream state.
			if !conn.emitted && conn.path != "" && len(conn.bodyBuf) > 0 {
				result = &CompleteRequest{
					Timestamp: timestamp,
					Path:      conn.path,
					Headers:   conn.headers,
					Body:      append([]byte(nil), conn.bodyBuf...),
				}
			}
			conn.headers = make(map[string]string)
			conn.path = ""
			conn.bodyBuf = conn.bodyBuf[:0]
			conn.emitted = false

			h := conn.headers
			conn.hpackDec.SetEmitFunc(func(f hpack.HeaderField) {
				h[f.Name] = f.Value
			})

			hpackData := framePayload
			if flags&0x8 != 0 && len(hpackData) > 0 { // PADDED
				padLen := int(hpackData[0])
				if padLen < len(hpackData) {
					hpackData = hpackData[1 : len(hpackData)-padLen]
				}
			}
			if flags&0x20 != 0 && len(hpackData) >= 5 { // PRIORITY
				hpackData = hpackData[5:]
			}
			conn.hpackDec.Write(hpackData)
			conn.path = conn.headers[":path"]

		case 0x9: // CONTINUATION
			ct.stats.FramesOther++
			conn.hpackDec.Write(framePayload)
			if conn.path == "" {
				conn.path = conn.headers[":path"]
			}

		case 0x0: // DATA
			ct.stats.FramesData++
			grpcPayload := framePayload
			if flags&0x8 != 0 && len(grpcPayload) > 0 { // PADDED
				padLen := int(grpcPayload[0])
				if padLen < len(grpcPayload) {
					grpcPayload = grpcPayload[1 : len(grpcPayload)-padLen]
				}
			}
			if len(grpcPayload) < 5 {
				ct.stats.DataTooShort++
			} else {
				compressed := grpcPayload[0]
				msgLen := binary.BigEndian.Uint32(grpcPayload[1:5])
				body := grpcPayload[5:]
				switch {
				case msgLen == 0:
					ct.stats.DataEmptyBody++
				case int(msgLen) > len(body):
					ct.stats.DataMsgLenMismatch++
				case compressed == 0:
					conn.bodyBuf = append(conn.bodyBuf, body[:msgLen]...)
				default:
					// Compressed — auto-detect codec from magic bytes.
					if decompressed, err := ct.registry.Decompress(body[:msgLen]); err == nil {
						conn.bodyBuf = append(conn.bodyBuf, decompressed...)
						ct.stats.DataDecompressOK++
					} else {
						ct.stats.DataUnknownCompression++
					}
				}
			}
			if !conn.emitted && conn.path != "" && len(conn.bodyBuf) > 0 {
				result = &CompleteRequest{
					Timestamp: timestamp,
					Path:      conn.path,
					Headers:   conn.headers,
					Body:      append([]byte(nil), conn.bodyBuf...),
				}
				conn.emitted = true
				ct.stats.DataEmitted++
			} else if ct.debug {
				if conn.path == "" && len(conn.bodyBuf) > 0 {
					ct.stats.DataNoPath++
				} else if conn.emitted && len(conn.bodyBuf) > 0 {
					ct.stats.DataAlreadyEmit++
				}
			}

		default:
			ct.stats.FramesOther++
		}

		offset += 9 + length
	}

	if offset > 0 {
		conn.rawBuf = append(conn.rawBuf[:0], conn.rawBuf[offset:]...)
	}
	return result
}
