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

package dejavu

// ChunkSource 描述同步时可选的只读分块来源。
//
// DownloadChunk 返回仓库中压缩并加密后的原始分块数据，调用方负责解密、解压和校验分块 ID。
type ChunkSource interface {
	Name() string
	HasChunks(ids []string) (ret map[string]bool, err error)
	DownloadChunk(id string) (data []byte, err error)
	GetConcurrentReqs() int
}

// ValidatingChunkSource 描述支持在多个来源之间按内容校验结果继续回退的分块来源。
type ValidatingChunkSource interface {
	ChunkSource
	DownloadChunkValidated(id string, validate func(data []byte) error) (data []byte, err error)
}

// ObjectSource 描述同步时可选的只读对象来源，除分块外还可以提供文件元数据对象。
type ObjectSource interface {
	ValidatingChunkSource
	HasObjects(ids []string) (ret map[string]bool, err error)
	DownloadObjectValidated(id string, validate func(data []byte) error) (data []byte, err error)
}

type chunkDownloadStat struct {
	CloudCount        int
	CloudBytes        int64
	PeerBytes         int64
	PeerCount         int
	PeerFallbackCount int
}
