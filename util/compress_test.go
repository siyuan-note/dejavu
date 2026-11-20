// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package util

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func encodeRepeat(t *testing.T, size int) []byte {
	t.Helper()
	var encoded bytes.Buffer
	writer, err := zstd.NewWriter(&encoded, zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithWindowSize(64*1024), zstd.WithEncoderCRC(false))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.CopyN(writer, bytes.NewReader(bytes.Repeat([]byte("x"), size)), int64(size)); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func TestDecodeBoundedZstdRejectsOversizedData(t *testing.T) {
	if _, err := DecodeBoundedZstd(make([]byte, 2048), 1024, 1<<20); !errors.Is(err, ErrCloudMetadataTooLarge) {
		t.Fatalf("compressed size limit was not reported as a size error: %v", err)
	}

	encoded := encodeRepeat(t, 1<<20)
	if _, err := DecodeBoundedZstd(encoded, 1<<20, 64<<10); !errors.Is(err, ErrCloudMetadataTooLarge) {
		t.Fatalf("decoded size limit was not reported as a size error: %v", err)
	}

	decoded, err := DecodeBoundedZstd(encoded, 1<<20, 2<<20)
	if err != nil || (1<<20) != len(decoded) {
		t.Fatalf("valid bounded data was rejected: len=%d, err=%v", len(decoded), err)
	}
	if _, err = DecodeCloudMetadata(encoded); err != nil {
		t.Fatalf("valid cloud metadata was rejected: %v", err)
	}
}
