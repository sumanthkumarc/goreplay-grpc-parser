// goreplay-grpc-parser: reads raw goreplay TCP captures from a Kafka topic,
// decodes gRPC requests using local proto files, and publishes parsed JSON to
// a destination Kafka topic.
//
// Usage:
//
//	./parser --source-brokers=... --source-topic=... --dest-brokers=... --dest-topic=... --proto-path=./proto
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/fullstorydev/grpcurl"
	"github.com/golang/snappy"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"golang.org/x/net/http2/hpack"
)

const (
	gorHeaderTypeRequest = "1"
	http2Preface         = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	defaultConsumerGroup = "goreplay-grpc-parser"
	defaultWorkers       = 4
	defaultWellKnownDir  = "/tmp/well-known-protos"
	defaultIdleTimeout   = 10 * time.Second
	logEvery             = 1000
)

type outputMessage struct {
	Timestamp int64             `json:"timestamp"`
	Method    string            `json:"method"`
	Headers   map[string]string `json:"headers"`
	Request   json.RawMessage   `json:"request"`
}

// completeRequest is a fully reassembled gRPC request ready for proto decoding.
type completeRequest struct {
	timestamp int64
	path      string
	headers   map[string]string
	body      []byte
}

// connState holds per-TCP-connection reassembly state.
type connState struct {
	hpackDec *hpack.Decoder
	rawBuf   []byte            // unprocessed TCP bytes waiting for a complete HTTP/2 frame
	headers  map[string]string // decoded from the most recent HEADERS frame
	path     string
	bodyBuf  []byte
	emitted  bool // true once we have emitted a request for the current stream
}

// trackerStats holds frame-level counters for debug output.
type trackerStats struct {
	rawMessages            int64
	nonRequest             int64 // goreplay type != "1"
	framesHeaders          int64
	framesData             int64
	framesOther            int64
	dataNoPath             int64 // DATA arrived but no :path known yet for this connection
	dataEmitted            int64
	dataEmptyBody          int64 // DATA frame with msgLen==0 (END_STREAM-only packet)
	dataAlreadyEmit        int64 // DATA frame blocked because conn.emitted already true
	dataTooShort           int64 // DATA frame payload < 5 bytes (no room for gRPC envelope)
	dataMsgLenMismatch     int64 // gRPC msgLen > available body bytes
	dataSnappyOK           int64 // compressed flag set, snappy decompression succeeded
	dataUnknownCompression int64 // compressed flag set, snappy failed (unknown encoding)
}

// connTracker reassembles goreplay TCP packets into complete gRPC requests.
// It must be used from a single goroutine.
type connTracker struct {
	conns map[string]*connState
	debug bool
	stats trackerStats
}

func newConnTracker(debug bool) *connTracker {
	return &connTracker{
		conns: make(map[string]*connState),
		debug: debug,
	}
}

// feed processes one raw goreplay message and returns a complete request if one
// is ready, or nil if more packets are needed.
func (ct *connTracker) feed(data []byte) *completeRequest {
	ct.stats.rawMessages++

	headerBytes, payload, ok := bytes.Cut(data, []byte("\n"))
	if !ok {
		return nil
	}

	parts := strings.Fields(string(headerBytes))
	if len(parts) < 2 {
		return nil
	}
	if parts[0] != gorHeaderTypeRequest {
		ct.stats.nonRequest++
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

	// Append this packet's bytes to the connection's raw buffer, then parse
	// complete frames. Any incomplete frame tail stays in rawBuf for next time.
	conn.rawBuf = append(conn.rawBuf, payload...)
	return ct.processFrames(conn, connID, timestamp)
}

func decompressSnappy(data []byte) ([]byte, error) {
	r := snappy.NewReader(bytes.NewReader(data))
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (ct *connTracker) getOrCreate(connID string) *connState {
	if conn, ok := ct.conns[connID]; ok {
		return conn
	}
	conn := &connState{
		hpackDec: hpack.NewDecoder(4096, nil),
		headers:  make(map[string]string),
	}
	ct.conns[connID] = conn
	return conn
}

func (ct *connTracker) processFrames(conn *connState, connID string, timestamp int64) *completeRequest {
	var result *completeRequest
	data := conn.rawBuf

	offset := 0
	for offset+9 <= len(data) {
		length := int(data[offset])<<16 | int(data[offset+1])<<8 | int(data[offset+2])
		frameType := data[offset+3]
		flags := data[offset+4]

		if offset+9+length > len(data) {
			break // incomplete frame — leave in rawBuf for next packet
		}
		framePayload := data[offset+9 : offset+9+length]

		switch frameType {
		case 0x1: // HEADERS — start of a new request on this connection
			ct.stats.framesHeaders++
			// If we had accumulated body for a previous stream but never emitted
			// (e.g. END_STREAM never arrived), flush it now before resetting.
			if !conn.emitted && conn.path != "" && len(conn.bodyBuf) > 0 {
				result = &completeRequest{
					timestamp: timestamp,
					path:      conn.path,
					headers:   conn.headers,
					body:      append([]byte(nil), conn.bodyBuf...),
				}
			}

			// Reset stream state; HPACK decoder is connection-scoped and persists.
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
			ct.stats.framesOther++
			conn.hpackDec.Write(framePayload)
			if conn.path == "" {
				conn.path = conn.headers[":path"]
			}

		case 0x0: // DATA
			ct.stats.framesData++
			grpcPayload := framePayload
			if flags&0x8 != 0 && len(grpcPayload) > 0 { // PADDED
				padLen := int(grpcPayload[0])
				if padLen < len(grpcPayload) {
					grpcPayload = grpcPayload[1 : len(grpcPayload)-padLen]
				}
			}
			if len(grpcPayload) < 5 {
				ct.stats.dataTooShort++
			} else {
				compressed := grpcPayload[0]
				msgLen := binary.BigEndian.Uint32(grpcPayload[1:5])
				body := grpcPayload[5:]
				if msgLen == 0 {
					ct.stats.dataEmptyBody++
				} else if int(msgLen) > len(body) {
					ct.stats.dataMsgLenMismatch++
				} else if compressed == 0 {
					conn.bodyBuf = append(conn.bodyBuf, body[:msgLen]...)
				} else {
					// compressed flag is set — try snappy (the only encoding we support).
					// If snappy fails, count as unknown compression and skip.
					if decompressed, err := decompressSnappy(body[:msgLen]); err == nil {
						conn.bodyBuf = append(conn.bodyBuf, decompressed...)
						ct.stats.dataSnappyOK++
					} else {
						ct.stats.dataUnknownCompression++
					}
				}
			}
			if !conn.emitted && conn.path != "" && len(conn.bodyBuf) > 0 {
				result = &completeRequest{
					timestamp: timestamp,
					path:      conn.path,
					headers:   conn.headers,
					body:      append([]byte(nil), conn.bodyBuf...),
				}
				conn.emitted = true
				ct.stats.dataEmitted++
			} else if ct.debug {
				if conn.path == "" && len(conn.bodyBuf) > 0 {
					ct.stats.dataNoPath++
				} else if conn.emitted && len(conn.bodyBuf) > 0 {
					ct.stats.dataAlreadyEmit++
				}
			}

		default:
			ct.stats.framesOther++
		}

		offset += 9 + length
	}

	// Discard fully consumed bytes; keep any partial frame for the next packet.
	if offset > 0 {
		conn.rawBuf = append(conn.rawBuf[:0], conn.rawBuf[offset:]...)
	}

	return result
}

// wellKnownProtos maps local relative path → download URL.
var wellKnownProtos = map[string]string{
	"google/api/annotations.proto":     "https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto",
	"google/api/http.proto":            "https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto",
	"google/protobuf/any.proto":        "https://raw.githubusercontent.com/protocolbuffers/protobuf/main/src/google/protobuf/any.proto",
	"google/protobuf/descriptor.proto": "https://raw.githubusercontent.com/protocolbuffers/protobuf/main/src/google/protobuf/descriptor.proto",
	"google/protobuf/duration.proto":   "https://raw.githubusercontent.com/protocolbuffers/protobuf/main/src/google/protobuf/duration.proto",
	"google/protobuf/struct.proto":     "https://raw.githubusercontent.com/protocolbuffers/protobuf/main/src/google/protobuf/struct.proto",
	"google/protobuf/timestamp.proto":  "https://raw.githubusercontent.com/protocolbuffers/protobuf/main/src/google/protobuf/timestamp.proto",
	"google/protobuf/wrappers.proto":   "https://raw.githubusercontent.com/protocolbuffers/protobuf/main/src/google/protobuf/wrappers.proto",
	"google/protobuf/empty.proto":      "https://raw.githubusercontent.com/protocolbuffers/protobuf/main/src/google/protobuf/empty.proto",
	"google/protobuf/field_mask.proto": "https://raw.githubusercontent.com/protocolbuffers/protobuf/main/src/google/protobuf/field_mask.proto",
	"validate/validate.proto":          "https://raw.githubusercontent.com/bufbuild/protoc-gen-validate/main/validate/validate.proto",
}

func main() {
	sourceBrokers := flag.String("source-brokers", "", "CSV Kafka broker addresses for source topic (required)")
	sourceTopic := flag.String("source-topic", "", "Source Kafka topic with raw goreplay binary (required)")
	destBrokers := flag.String("dest-brokers", "", "CSV Kafka broker addresses for destination topic (required)")
	destTopic := flag.String("dest-topic", "", "Destination Kafka topic for parsed JSON (required)")
	protoPath := flag.String("proto-path", "", "Path to proto/ directory (required)")
	wellKnownPath := flag.String("well-known-path", "", "Path to well-known protos dir (optional, auto-downloads to /tmp if unset)")
	consumerGroup := flag.String("consumer-group", defaultConsumerGroup, "Kafka consumer group ID")
	workers := flag.Int("workers", defaultWorkers, "Number of parallel message processing goroutines")
	idleTimeout := flag.Duration("idle-timeout", defaultIdleTimeout, "Exit after this long with no new messages (0 = run forever)")
	debug := flag.Bool("debug", false, "Print frame-level diagnostic stats on exit; reads from beginning, no Kafka output")
	startOffset := flag.Int64("offset", -1, "Start reading from this partition offset, bypassing committed group offsets (-1 = use group offsets / auto.offset.reset)")
	flag.Parse()

	if *sourceBrokers == "" || *sourceTopic == "" || *destBrokers == "" || *destTopic == "" || *protoPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	wkPath, err := resolveWellKnownPath(*wellKnownPath)
	if err != nil {
		log.Fatalf("Failed to set up well-known protos: %v", err)
	}

	source, err := loadDescriptorSource(*protoPath, wkPath)
	if err != nil {
		log.Fatalf("Failed to load proto descriptors: %v", err)
	}
	log.Println("Proto descriptors loaded successfully")

	var producer *kafka.Producer
	if !*debug {
		var err error
		producer, err = newProducer(*destBrokers)
		if err != nil {
			log.Fatalf("Failed to create Kafka producer: %v", err)
		}
		defer func() {
			producer.Flush(5000)
			producer.Close()
		}()
		go drainDeliveryReports(producer)
	}

	consumer, err := newConsumer(*sourceBrokers, *consumerGroup, *debug)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	if *startOffset >= 0 || *debug {
		// Use explicit partition assignment to bypass committed group offsets.
		offset := kafka.OffsetBeginning
		if *startOffset >= 0 {
			offset = kafka.Offset(*startOffset)
		}
		if err := consumer.Assign([]kafka.TopicPartition{{
			Topic:     sourceTopic,
			Partition: 0,
			Offset:    offset,
		}}); err != nil {
			log.Fatalf("Failed to assign partition: %v", err)
		}
		if *debug {
			log.Printf("DEBUG mode: reading %s from offset %v, no output to Kafka", *sourceTopic, offset)
		} else {
			log.Printf("Consuming from %s offset %v with %d workers, publishing to %s", *sourceTopic, offset, *workers, *destTopic)
		}
	} else {
		if err := consumer.SubscribeTopics([]string{*sourceTopic}, nil); err != nil {
			log.Fatalf("Failed to subscribe to topic %s: %v", *sourceTopic, err)
		}
		log.Printf("Consuming from %s with %d workers, publishing to %s", *sourceTopic, *workers, *destTopic)
	}
	if *idleTimeout > 0 {
		log.Printf("Will exit after %v of no new messages", *idleTimeout)
	}

	completeChan := make(chan *completeRequest, (*workers)*10)
	var wg sync.WaitGroup
	var msgCount int64
	var skipCount int64

	pathStats := make(map[string]int64)
	var pathMu sync.Mutex

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Worker panic: %v", r)
				}
			}()
			for req := range completeChan {
				result, err := decodeRequest(req, source)
				if err != nil {
					s := atomic.AddInt64(&skipCount, 1)
					if s%logEvery == 0 {
						log.Printf("Skipped %d requests (last: %v)", s, err)
					}
					continue
				}
				jsonBytes, err := json.Marshal(result)
				if err != nil {
					log.Printf("JSON marshal error: %v", err)
					continue
				}
				if producer != nil {
					if err := producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: destTopic, Partition: kafka.PartitionAny},
						Value:          jsonBytes,
					}, nil); err != nil {
						log.Printf("Produce error: %v", err)
					}
				}
				pathMu.Lock()
				pathStats[result.Method]++
				pathMu.Unlock()
				n := atomic.AddInt64(&msgCount, 1)
				if n%logEvery == 0 {
					log.Printf("Processed %d messages (skipped %d)", n, atomic.LoadInt64(&skipCount))
				}
			}
		}()
	}

	// Connection tracking runs on the main goroutine (single-threaded) so no
	// locking is needed for the HPACK decoders or connection state.
	tracker := newConnTracker(*debug)

	// pollMs is the ReadMessage timeout. When idle-timeout is set we poll in
	// short bursts so we can detect the idle condition promptly.
	pollMs := 1000
	if *idleTimeout == 0 {
		pollMs = -1 // block indefinitely
	}

	lastMsg := time.Now()
	for {
		msg, err := consumer.ReadMessage(time.Duration(pollMs) * time.Millisecond)
		if err != nil {
			if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
				if *idleTimeout > 0 && time.Since(lastMsg) >= *idleTimeout {
					log.Printf("No new messages for %v — exiting", *idleTimeout)
					break
				}
				continue
			}
			log.Printf("Kafka consume error: %v", err)
			continue
		}
		lastMsg = time.Now()
		if req := tracker.feed(msg.Value); req != nil {
			completeChan <- req
		}
	}

	close(completeChan)
	wg.Wait()

	printStats(atomic.LoadInt64(&msgCount), atomic.LoadInt64(&skipCount), pathStats)
	if *debug {
		printDebugStats(&tracker.stats)
	}
}

func printDebugStats(s *trackerStats) {
	log.Printf("=== Debug: Frame-level Stats ===")
	log.Printf("  Raw Kafka messages:          %d", s.rawMessages)
	log.Printf("  Non-request (type!=1):       %d", s.nonRequest)
	log.Printf("  HEADERS frames:              %d", s.framesHeaders)
	log.Printf("  DATA frames:                 %d", s.framesData)
	log.Printf("  Other frames:                %d", s.framesOther)
	log.Printf("  DATA emitted:                %d", s.dataEmitted)
	log.Printf("  DATA empty body:             %d", s.dataEmptyBody)
	log.Printf("  DATA too short (<5b):        %d", s.dataTooShort)
	log.Printf("  DATA msgLen mismatch:        %d", s.dataMsgLenMismatch)
	log.Printf("  DATA snappy OK:              %d", s.dataSnappyOK)
	log.Printf("  DATA unknown compression:    %d", s.dataUnknownCompression)
	log.Printf("  DATA no-path (orphan):       %d", s.dataNoPath)
	log.Printf("  DATA already emitted:        %d", s.dataAlreadyEmit)
}

func printStats(processed, skipped int64, pathStats map[string]int64) {
	log.Printf("=== Final Stats ===")
	log.Printf("Processed: %d  Skipped: %d  Total: %d", processed, skipped, processed+skipped)
	if len(pathStats) == 0 {
		return
	}

	type entry struct {
		path  string
		count int64
	}
	entries := make([]entry, 0, len(pathStats))
	for p, c := range pathStats {
		entries = append(entries, entry{p, c})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].count > entries[j].count })

	log.Printf("--- Per-method breakdown ---")
	for _, e := range entries {
		log.Printf("  %6d  %s", e.count, e.path)
	}
}

func resolveWellKnownPath(flagVal string) (string, error) {
	if flagVal != "" {
		if _, err := os.Stat(flagVal); err == nil {
			log.Printf("Using well-known protos from: %s", flagVal)
			return flagVal, nil
		}
		log.Printf("Well-known path %q not found, downloading to %s", flagVal, defaultWellKnownDir)
	}
	return downloadWellKnownProtos(defaultWellKnownDir)
}

func downloadWellKnownProtos(dir string) (string, error) {
	log.Printf("Setting up well-known protos in %s", dir)
	for relPath, url := range wellKnownProtos {
		destFile := filepath.Join(dir, relPath)
		if _, err := os.Stat(destFile); err == nil {
			continue // already present
		}
		if err := os.MkdirAll(filepath.Dir(destFile), 0755); err != nil {
			return "", fmt.Errorf("mkdir %s: %w", filepath.Dir(destFile), err)
		}
		if err := downloadFile(url, destFile); err != nil {
			return "", fmt.Errorf("download %s: %w", relPath, err)
		}
		log.Printf("Downloaded %s", relPath)
	}
	return dir, nil
}

func downloadFile(url, dest string) error {
	resp, err := http.Get(url) //nolint:gosec
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, resp.Body)
	return err
}

func loadDescriptorSource(protoPath, wellKnownPath string) (grpcurl.DescriptorSource, error) {
	// Proto imports are like "proto/myservice/service.proto", so the import root
	// must be the parent of the proto/ directory.
	protoRoot := filepath.Dir(filepath.Clean(protoPath))
	importPaths := []string{protoRoot, wellKnownPath}

	var protoFiles []string
	for _, absPath := range findProtos(protoPath) {
		rel, err := filepath.Rel(protoRoot, absPath)
		if err != nil {
			return nil, fmt.Errorf("rel path error for %s: %w", absPath, err)
		}
		protoFiles = append(protoFiles, rel)
	}
	log.Printf("Loading %d proto files from %s", len(protoFiles), protoPath)
	return grpcurl.DescriptorSourceFromProtoFiles(importPaths, protoFiles...)
}

// decodeRequest decodes a fully reassembled gRPC request into an outputMessage.
func decodeRequest(req *completeRequest, source grpcurl.DescriptorSource) (*outputMessage, error) {
	symbolName := grpcPathToMethod(req.path)
	md, err := source.FindSymbol(symbolName)
	if err != nil {
		return nil, fmt.Errorf("method %s not found in protos: %w", symbolName, err)
	}
	methodDesc, ok := md.(*desc.MethodDescriptor)
	if !ok {
		return nil, fmt.Errorf("%s is not a method descriptor", symbolName)
	}

	dynMsg := dynamic.NewMessage(methodDesc.GetInputType())
	if err := dynMsg.Unmarshal(req.body); err != nil {
		return nil, fmt.Errorf("protobuf unmarshal failed: %w", err)
	}
	jsonBytes, err := dynMsg.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("JSON marshal failed: %w", err)
	}

	skipHeaders := map[string]bool{":method": true, ":path": true, ":scheme": true, ":authority": true}
	outputHeaders := make(map[string]string, len(req.headers))
	for k, v := range req.headers {
		if !skipHeaders[k] {
			outputHeaders[k] = v
		}
	}

	return &outputMessage{
		Timestamp: req.timestamp,
		Method:    strings.TrimPrefix(req.path, "/"),
		Headers:   outputHeaders,
		Request:   json.RawMessage(jsonBytes),
	}, nil
}

// grpcPathToMethod converts /package.Service/Method to package.Service.Method
// for use as a proto descriptor symbol lookup key.
func grpcPathToMethod(path string) string {
	path = strings.TrimPrefix(path, "/")
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		return path
	}
	return path[:idx] + "." + path[idx+1:]
}

// findProtos recursively returns absolute paths of all .proto files under dir.
func findProtos(dir string) []string {
	var result []string
	entries, err := os.ReadDir(dir)
	if err != nil {
		return result
	}
	for _, e := range entries {
		path := filepath.Join(dir, e.Name())
		if e.IsDir() {
			result = append(result, findProtos(path)...)
		} else if strings.HasSuffix(e.Name(), ".proto") {
			result = append(result, path)
		}
	}
	return result
}

func newConsumer(brokers, group string, debug bool) (*kafka.Consumer, error) {
	return kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           group,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": !debug, // don't advance offsets in debug mode
	})
}

func newProducer(brokers string) (*kafka.Producer, error) {
	return kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
}

// drainDeliveryReports reads delivery events to prevent producer.Produce() from blocking.
func drainDeliveryReports(p *kafka.Producer) {
	for e := range p.Events() {
		if msg, ok := e.(*kafka.Message); ok {
			if msg.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v", msg.TopicPartition.Error)
			}
		}
	}
}
