package output

import (
	"bufio"
	"fmt"
	"os"
	"sync"
)

// FileWriter writes each decoded message as a JSON line to a file.
// Use "-" as path to write to stdout. Safe for concurrent use.
type FileWriter struct {
	mu  sync.Mutex
	f   *os.File
	buf *bufio.Writer
}

// NewFileWriter opens path for writing (truncates if it exists).
// Pass "-" to write to stdout.
func NewFileWriter(path string) (*FileWriter, error) {
	var f *os.File
	if path == "-" {
		f = os.Stdout
	} else {
		var err error
		f, err = os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("create output file: %w", err)
		}
	}
	return &FileWriter{f: f, buf: bufio.NewWriter(f)}, nil
}

// Write appends jsonBytes followed by a newline.
func (w *FileWriter) Write(jsonBytes []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.buf.Write(jsonBytes); err != nil {
		return err
	}
	return w.buf.WriteByte('\n')
}

// Close flushes the buffer and closes the file (stdout is flushed but not closed).
func (w *FileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.buf.Flush(); err != nil {
		return err
	}
	if w.f == os.Stdout {
		return nil
	}
	return w.f.Close()
}
