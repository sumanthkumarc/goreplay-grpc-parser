package input

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// FileReader reads raw goreplay messages from a flat file.
//
// The file must be in the goreplay text dump format: each message starts with
// a header line ("1 <connID> <timestamp> <latency>") followed by the raw
// binary payload encoded as a hex string on the next line, repeated per
// message. Lines beginning with ">" indicate message boundaries in some
// goreplay dump formats and are treated as headers when they match.
//
// Example file format:
//
//	1 5b8817ac0a09ad5b 1775544696692543319 0
//	<hex-encoded binary payload>
//	1 5b8817ac0a09ad5b 1775544696692543320 0
//	<hex-encoded binary payload>
//
// Read returns ErrTimeout when the file is exhausted — the caller's idle
// timeout will then trigger a clean exit, just like an idle Kafka topic.
type FileReader struct {
	scanner *bufio.Scanner
	f       *os.File
}

// NewFileReader opens path and returns a FileReader ready to read messages.
func NewFileReader(path string) (*FileReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open input file: %w", err)
	}
	return &FileReader{
		scanner: bufio.NewScanner(f),
		f:       f,
	}, nil
}

// Read returns the next message from the file. timeout is ignored (file I/O
// is synchronous). Returns ErrTimeout when the file is exhausted.
func (r *FileReader) Read(_ time.Duration) (*Message, error) {
	// Each message is two logical parts: a goreplay header line and a payload.
	// The payload may be raw binary or hex-encoded depending on how goreplay
	// wrote the dump. We try hex-decode first; if that fails we use the raw bytes.
	headerLine, err := r.readLine()
	if err == io.EOF {
		return nil, ErrTimeout{}
	}
	if err != nil {
		return nil, err
	}

	// Skip blank lines between messages.
	for strings.TrimSpace(headerLine) == "" {
		headerLine, err = r.readLine()
		if err == io.EOF {
			return nil, ErrTimeout{}
		}
		if err != nil {
			return nil, err
		}
	}

	payloadLine, err := r.readLine()
	if err == io.EOF {
		// Header with no payload — skip.
		return nil, ErrTimeout{}
	}
	if err != nil {
		return nil, err
	}

	payload, decErr := hex.DecodeString(strings.TrimSpace(payloadLine))
	if decErr != nil {
		// Not hex — treat as raw bytes.
		payload = []byte(payloadLine)
	}

	// Reassemble into the goreplay wire format that tracker.Feed expects:
	// "<header>\n<payload>"
	msg := make([]byte, 0, len(headerLine)+1+len(payload))
	msg = append(msg, []byte(headerLine)...)
	msg = append(msg, '\n')
	msg = append(msg, payload...)

	// Sanity-check: must look like a goreplay header.
	if !bytes.ContainsAny(msg[:min(len(msg), 2)], "123") {
		return nil, fmt.Errorf("unexpected file line (not a goreplay header): %q", headerLine)
	}

	return &Message{Value: msg}, nil
}

// Close closes the underlying file.
func (r *FileReader) Close() error {
	return r.f.Close()
}

func (r *FileReader) readLine() (string, error) {
	if r.scanner.Scan() {
		return r.scanner.Text(), nil
	}
	if err := r.scanner.Err(); err != nil {
		return "", err
	}
	return "", io.EOF
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
