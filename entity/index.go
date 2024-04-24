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

import (
	"fmt"
	"time"

	"github.com/88250/go-humanize"
)

// Index 描述了快照索引。
type Index struct {
	ID           string   `json:"id"`           // Hash
	Memo         string   `json:"memo"`         // 索引备注
	Created      int64    `json:"created"`      // 索引时间
	Files        []string `json:"files"`        // 文件列表
	Count        int      `json:"count"`        // 文件总数
	Size         int64    `json:"size"`         // 文件总大小
	SystemID     string   `json:"systemID"`     // 系统 ID
	SystemName   string   `json:"systemName"`   // 系统名称
	SystemOS     string   `json:"systemOS"`     // 系统操作系统
	CheckIndexID string   `json:"checkIndexID"` // Check Index ID
}

func (index *Index) String() string {
	return fmt.Sprintf("device=%s/%s, id=%s, files=%d, size=%s, created=%s",
		index.SystemID, index.SystemOS, index.ID, len(index.Files), humanize.BytesCustomCeil(uint64(index.Size), 2), time.UnixMilli(index.Created).Format("2006-01-02 15:04:05"))
}

// CheckIndex 描述了一个 Index 对应的数据 ID，包括 File ID 和 Chunk ID。
//
// 该结构体在数据同步云端时根据本地 Latest Index 生成，在云端服务上用于校验数据完整性。
//
// 该数据结构仅保存 ID，因此不会影响端到端加密的安全性，不会对数据安全造成任何影响。
//
// 存放路径：repo/check/indexes/{id}。
type CheckIndex struct {
	ID      string            `json:"id"`      // Hash
	IndexID string            `json:"indexID"` // Index ID
	Files   []*CheckIndexFile `json:"files"`   // File IDs
}

type CheckIndexFile struct {
	ID     string   `json:"id"`     // File ID
	Chunks []string `json:"chunks"` // Chunk IDs
}

type CheckReport struct {
	CheckTime      int64    `json:"checkTime"`
	CheckCount     int      `json:"checkCount"`
	FixCount       int      `json:"fixCount"`
	MissingObjects []string `json:"missingObjects"`
}
