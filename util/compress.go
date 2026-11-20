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
	"errors"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

const (
	// MaxCloudMetadataCompressedBytes 限制云端元数据对象的压缩体积
	MaxCloudMetadataCompressedBytes = 64 << 20
	// MaxCloudMetadataDecodedBytes 限制云端元数据解压后的体积，避免高压缩比对象在解析前耗尽内存
	MaxCloudMetadataDecodedBytes = 64 << 20
)

// ErrCloudMetadataTooLarge 表示云端元数据超过体积上限，重试不会改变结果。
var ErrCloudMetadataTooLarge = errors.New("cloud metadata exceeds size limit")

// DecodeCloudMetadata 解压云端元数据并限制压缩前后体积，避免不可信对象在解析前耗尽内存。
func DecodeCloudMetadata(data []byte) ([]byte, error) {
	return DecodeBoundedZstd(data, MaxCloudMetadataCompressedBytes, MaxCloudMetadataDecodedBytes)
}

// DecodeBoundedZstd 解压 zstd 数据并限制压缩前后体积。
func DecodeBoundedZstd(data []byte, maxCompressed, maxDecoded int) ([]byte, error) {
	if len(data) > maxCompressed {
		return nil, fmt.Errorf("%w: compressed data exceeds %d bytes", ErrCloudMetadataTooLarge, maxCompressed)
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(uint64(maxDecoded)))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	decoded, err := decoder.DecodeAll(data, nil)
	if err != nil {
		if errors.Is(err, zstd.ErrDecoderSizeExceeded) {
			return nil, fmt.Errorf("%w: decoded data exceeds %d bytes", ErrCloudMetadataTooLarge, maxDecoded)
		}
		return nil, err
	}
	if len(decoded) > maxDecoded {
		return nil, fmt.Errorf("%w: decoded data exceeds %d bytes", ErrCloudMetadataTooLarge, maxDecoded)
	}
	return decoded, nil
}
