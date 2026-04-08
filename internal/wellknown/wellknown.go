// Package wellknown manages the well-known proto files (google/api,
// google/protobuf, validate) that are commonly imported by gRPC services.
// If the files are not present locally they are downloaded at startup.
package wellknown

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

const DefaultDir = "/tmp/well-known-protos"

// protos maps each relative proto path to its canonical download URL.
var protos = map[string]string{
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

// Resolve returns a local directory containing the well-known proto files.
// If flagVal points to an existing directory it is used directly. Otherwise
// the files are downloaded to DefaultDir.
func Resolve(flagVal string) (string, error) {
	if flagVal != "" {
		if _, err := os.Stat(flagVal); err == nil {
			log.Printf("Using well-known protos from: %s", flagVal)
			return flagVal, nil
		}
		log.Printf("Well-known path %q not found, downloading to %s", flagVal, DefaultDir)
	}
	return download(DefaultDir)
}

func download(dir string) (string, error) {
	log.Printf("Setting up well-known protos in %s", dir)
	for relPath, url := range protos {
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
