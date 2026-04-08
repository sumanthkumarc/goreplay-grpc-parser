package compression

import (
	"bytes"
	"fmt"
	"io"

	"github.com/golang/snappy"
)

// snappyMagic is the stream identifier chunk for the Snappy framing format.
// See https://github.com/google/snappy/blob/main/framing_format.txt
var snappyMagic = []byte{0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59}

// SnappyDecompressor decodes Snappy framing-format streams (grpc-encoding: snappy).
type SnappyDecompressor struct{}

func (SnappyDecompressor) Magic() []byte { return snappyMagic }

func (SnappyDecompressor) Decompress(src []byte) ([]byte, error) {
	r := snappy.NewReader(bytes.NewReader(src))
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, fmt.Errorf("snappy decompress: %w", err)
	}
	return buf.Bytes(), nil
}
