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

package entity

import "github.com/siyuan-note/dejavu/util"

// Chunk 描述了文件分块
type Chunk struct {
	ID   string `json:"id"`
	Data []byte `json:"data"` // 实际的数据
}

func NewChunk(data []byte) *Chunk {
	chunkHash := util.Hash(data)
	return &Chunk{ID: chunkHash, Data: data}
}
