// Package input defines the Reader interface for consuming raw goreplay
// messages, along with built-in implementations for Kafka and file sources.
package input

import "time"

// Message is a raw goreplay message read from a source.
type Message struct {
	Value []byte
}

// Reader is the interface for a source of raw goreplay messages.
// Implementations must be safe for use from a single goroutine.
type Reader interface {
	// Read blocks until the next message is available or the idle timeout
	// elapses. Returns (msg, nil) on success.
	// Returns (nil, ErrTimeout) when no message arrived within the timeout;
	// callers should check idle conditions and retry.
	// Returns (nil, err) on unrecoverable errors.
	Read(timeout time.Duration) (*Message, error)

	// Close releases any resources held by the reader.
	Close() error
}

// ErrTimeout is returned by Read when no message was available within the
// given timeout. It is not fatal — the caller should check idle state and loop.
type ErrTimeout struct{}

func (ErrTimeout) Error() string { return "read timeout" }
