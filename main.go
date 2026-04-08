// goreplay-grpc-parser: reads raw goreplay TCP captures from an input source,
// decodes gRPC requests using local proto files, and publishes parsed JSON to
// a destination Kafka topic.
//
// Usage:
//
//	./parser --source-brokers=... --source-topic=... --dest-brokers=... --dest-topic=... --proto-path=./proto
//	./parser --source-file=dump.bin  --dest-brokers=... --dest-topic=... --proto-path=./proto
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sumanthkumarc/goreplay-grpc-parser/internal/decoder"
	"github.com/sumanthkumarc/goreplay-grpc-parser/internal/input"
	"github.com/sumanthkumarc/goreplay-grpc-parser/internal/output"
	"github.com/sumanthkumarc/goreplay-grpc-parser/internal/tracker"
	"github.com/sumanthkumarc/goreplay-grpc-parser/internal/wellknown"
)

const (
	defaultWorkers     = 4
	defaultIdleTimeout = 10 * time.Second
	logEvery           = 1000
)

func main() {
	// Source — mutually exclusive: Kafka or file.
	sourceBrokers := flag.String("source-brokers", "", "CSV Kafka broker addresses for source topic")
	sourceTopic := flag.String("source-topic", "", "Source Kafka topic with raw goreplay binary")
	sourceFile := flag.String("source-file", "", "Path to a goreplay dump file (alternative to Kafka source)")

	// Destination — mutually exclusive: Kafka or file.
	destBrokers := flag.String("dest-brokers", "", "CSV Kafka broker addresses for destination topic")
	destTopic := flag.String("dest-topic", "", "Destination Kafka topic for parsed JSON")
	destFile := flag.String("dest-file", "", "Path to output file for parsed JSON lines; use - for stdout (alternative to Kafka dest)")

	// Proto.
	protoPath := flag.String("proto-path", "", "Path to proto/ directory (required)")
	wellKnownPath := flag.String("well-known-path", "", "Path to well-known protos dir (optional, auto-downloads to /tmp if unset)")

	// Consumer tuning.
	consumerGroup := flag.String("consumer-group", "", "Kafka consumer group ID (required)")
	workers := flag.Int("workers", defaultWorkers, "Number of parallel message processing goroutines")
	idleTimeout := flag.Duration("idle-timeout", defaultIdleTimeout, "Exit after this long with no new messages (0 = run forever)")
	debug := flag.Bool("debug", false, "Print frame-level diagnostic stats on exit; reads from beginning, no Kafka output")
	startOffset := flag.Int64("offset", -1, "Kafka: start from this partition offset (-1 = use group offsets / auto.offset.reset)")
	flag.Parse()

	if *protoPath == "" {
		flag.Usage()
		os.Exit(1)
	}
	if *sourceFile == "" && *consumerGroup == "" {
		log.Fatal("--consumer-group is required when using Kafka source")
	}
	if *sourceFile == "" && (*sourceBrokers == "" || *sourceTopic == "") {
		log.Fatal("Provide either --source-file or both --source-brokers and --source-topic")
	}
	if *destFile == "" && (*destBrokers == "" || *destTopic == "") {
		log.Fatal("Provide either --dest-file or both --dest-brokers and --dest-topic")
	}

	wkPath, err := wellknown.Resolve(*wellKnownPath)
	if err != nil {
		log.Fatalf("Failed to set up well-known protos: %v", err)
	}

	source, err := decoder.LoadDescriptorSource(*protoPath, wkPath)
	if err != nil {
		log.Fatalf("Failed to load proto descriptors: %v", err)
	}
	log.Printf("Proto descriptors loaded successfully")

	var writer output.Writer
	if !*debug {
		if *destFile != "" {
			w, err := output.NewFileWriter(*destFile)
			if err != nil {
				log.Fatalf("Failed to open dest file: %v", err)
			}
			log.Printf("Writing output to file: %s", *destFile)
			writer = w
		} else {
			w, err := output.NewKafkaWriter(*destBrokers, *destTopic)
			if err != nil {
				log.Fatalf("Failed to create Kafka writer: %v", err)
			}
			writer = w
		}
		defer writer.Close()
	}

	// Build the input reader.
	var reader input.Reader
	if *sourceFile != "" {
		r, err := input.NewFileReader(*sourceFile)
		if err != nil {
			log.Fatalf("Failed to open source file: %v", err)
		}
		log.Printf("Reading from file: %s", *sourceFile)
		reader = r
	} else {
		r, err := input.NewKafkaReader(input.KafkaConfig{
			Brokers:       *sourceBrokers,
			Topic:         *sourceTopic,
			ConsumerGroup: *consumerGroup,
			StartOffset:   *startOffset,
			Debug:         *debug,
		})
		if err != nil {
			log.Fatalf("Failed to create Kafka reader: %v", err)
		}
		if *debug {
			log.Printf("DEBUG mode: reading %s from beginning, no output written", *sourceTopic)
		} else {
			log.Printf("Consuming from %s with %d workers", *sourceTopic, *workers)
		}
		reader = r
	}
	defer reader.Close()

	if *idleTimeout > 0 {
		log.Printf("Will exit after %v of no new messages", *idleTimeout)
	}

	completeChan := make(chan *tracker.CompleteRequest, (*workers)*10)
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
				result, err := decoder.Decode(req, source)
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
				if writer != nil {
					if err := writer.Write(jsonBytes); err != nil {
						log.Printf("Write error: %v", err)
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

	ct := tracker.New(*debug, nil) // nil = use default compression registry (Snappy)

	pollTimeout := time.Second
	if *idleTimeout == 0 {
		pollTimeout = 24 * time.Hour // effectively block forever
	}

	var lastMsg time.Time // zero until first message received
	for {
		msg, err := reader.Read(pollTimeout)
		if err != nil {
			if errors.As(err, &input.ErrTimeout{}) {
				if *idleTimeout > 0 && !lastMsg.IsZero() && time.Since(lastMsg) >= *idleTimeout {
					log.Printf("No new messages for %v — exiting", *idleTimeout)
					break
				}
				continue
			}
			log.Printf("Read error: %v", err)
			continue
		}
		lastMsg = time.Now()
		if req := ct.Feed(msg.Value); req != nil {
			completeChan <- req
		}
	}

	close(completeChan)
	wg.Wait()

	stats := ct.Stats()
	printStats(atomic.LoadInt64(&msgCount), atomic.LoadInt64(&skipCount), pathStats)
	if *debug {
		printDebugStats(&stats)
	}
}

func printDebugStats(s *tracker.Stats) {
	log.Printf("=== Debug: Frame-level Stats ===")
	log.Printf("  Raw Kafka messages:          %d", s.RawMessages)
	log.Printf("  Non-request (type!=1):       %d", s.NonRequest)
	log.Printf("  HEADERS frames:              %d", s.FramesHeaders)
	log.Printf("  DATA frames:                 %d", s.FramesData)
	log.Printf("  Other frames:                %d", s.FramesOther)
	log.Printf("  DATA emitted:                %d", s.DataEmitted)
	log.Printf("  DATA empty body:             %d", s.DataEmptyBody)
	log.Printf("  DATA too short (<5b):        %d", s.DataTooShort)
	log.Printf("  DATA msgLen mismatch:        %d", s.DataMsgLenMismatch)
	log.Printf("  DATA decompressed OK:        %d", s.DataDecompressOK)
	log.Printf("  DATA unknown compression:    %d", s.DataUnknownCompression)
	log.Printf("  DATA no-path (orphan):       %d", s.DataNoPath)
	log.Printf("  DATA already emitted:        %d", s.DataAlreadyEmit)
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
