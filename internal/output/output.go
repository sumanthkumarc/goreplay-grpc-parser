// Package output defines the Writer interface for publishing decoded gRPC
// messages, along with built-in implementations for Kafka and file targets.
package output

// Writer is the interface for a destination of decoded gRPC messages.
type Writer interface {
	// Write publishes one JSON-encoded message.
	Write(jsonBytes []byte) error

	// Close flushes any buffered data and releases resources.
	Close() error
}
