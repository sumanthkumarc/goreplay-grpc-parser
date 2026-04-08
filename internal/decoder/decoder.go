// Package decoder handles dynamic protobuf decoding of gRPC requests using
// runtime-loaded proto file descriptors. No code generation is required.
package decoder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fullstorydev/grpcurl"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/sumanthkumarc/goreplay-grpc-parser/internal/tracker"
)

// OutputMessage is the JSON structure published to the destination Kafka topic.
type OutputMessage struct {
	Timestamp int64             `json:"timestamp"`
	Method    string            `json:"method"`
	Headers   map[string]string `json:"headers"`
	Request   json.RawMessage   `json:"request"`
}

// pseudoHeaders are HTTP/2 pseudo-headers that are excluded from the output.
var pseudoHeaders = map[string]bool{
	":method":    true,
	":path":      true,
	":scheme":    true,
	":authority": true,
}

// Decode converts a CompleteRequest into an OutputMessage using the provided
// descriptor source for dynamic protobuf decoding.
func Decode(req *tracker.CompleteRequest, source grpcurl.DescriptorSource) (*OutputMessage, error) {
	symbolName := grpcPathToMethod(req.Path)
	md, err := source.FindSymbol(symbolName)
	if err != nil {
		return nil, fmt.Errorf("method %s not found in protos: %w", symbolName, err)
	}
	methodDesc, ok := md.(*desc.MethodDescriptor)
	if !ok {
		return nil, fmt.Errorf("%s is not a method descriptor", symbolName)
	}

	dynMsg := dynamic.NewMessage(methodDesc.GetInputType())
	if err := dynMsg.Unmarshal(req.Body); err != nil {
		return nil, fmt.Errorf("protobuf unmarshal failed: %w", err)
	}
	jsonBytes, err := dynMsg.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("JSON marshal failed: %w", err)
	}

	outputHeaders := make(map[string]string, len(req.Headers))
	for k, v := range req.Headers {
		if !pseudoHeaders[k] {
			outputHeaders[k] = v
		}
	}

	return &OutputMessage{
		Timestamp: req.Timestamp,
		Method:    strings.TrimPrefix(req.Path, "/"),
		Headers:   outputHeaders,
		Request:   json.RawMessage(jsonBytes),
	}, nil
}

// LoadDescriptorSource parses all .proto files under protoPath and returns a
// DescriptorSource for runtime symbol lookup.
func LoadDescriptorSource(protoPath, wellKnownPath string) (grpcurl.DescriptorSource, error) {
	// Proto imports use paths like "proto/myservice/service.proto", so the
	// import root must be the parent of the proto/ directory.
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
	return grpcurl.DescriptorSourceFromProtoFiles(importPaths, protoFiles...)
}

// grpcPathToMethod converts /package.Service/Method → package.Service.Method
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
