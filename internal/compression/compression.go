// Package compression defines the Decompressor interface and a Registry for
// pluggable decompression. Each implementation identifies itself by magic bytes
// so that the correct codec can be auto-selected from the raw payload.
//
// To add a new codec, implement Decompressor in its own file and register it
// with NewRegistry or add it to Default().
package compression

import (
	"bytes"
	"fmt"
)

// Decompressor is the interface implemented by each compression codec.
type Decompressor interface {
	// Magic returns the byte prefix that identifies this codec's stream format.
	// The registry uses this to auto-detect the correct decompressor.
	// Return nil to disable magic-based auto-detection.
	Magic() []byte

	// Decompress decompresses src and returns the original bytes.
	Decompress(src []byte) ([]byte, error)
}

// Registry holds a set of Decompressor implementations and selects the right
// one automatically based on magic-byte matching.
type Registry struct {
	codecs []Decompressor
}

// NewRegistry returns a Registry pre-populated with the provided codecs.
func NewRegistry(codecs ...Decompressor) *Registry {
	return &Registry{codecs: codecs}
}

// Default returns a Registry with only the Snappy codec registered.
func Default() *Registry {
	return NewRegistry(SnappyDecompressor{})
}

// Decompress auto-detects the codec from src's magic bytes and decompresses.
// Returns an error if no registered codec matches.
func (r *Registry) Decompress(src []byte) ([]byte, error) {
	for _, c := range r.codecs {
		if m := c.Magic(); len(m) > 0 && bytes.HasPrefix(src, m) {
			return c.Decompress(src)
		}
	}
	return nil, fmt.Errorf("no registered codec matched payload (len=%d, prefix=%x)", len(src), preview(src))
}

func preview(b []byte) []byte {
	if len(b) > 8 {
		return b[:8]
	}
	return b
}
