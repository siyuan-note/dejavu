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

// Index 描述了快照索引。
type Index struct {
	ID         string   `json:"id"`         // Hash
	Memo       string   `json:"memo"`       // 索引备注
	Created    int64    `json:"created"`    // 索引时间
	Files      []string `json:"files"`      // 文件列表
	Count      int      `json:"count"`      // 文件总数
	Size       int64    `json:"size"`       // 文件总大小
	SystemID   string   `json:"systemID"`   // 系统 ID
	SystemName string   `json:"systemName"` // 系统名称
	SystemOS   string   `json:"systemOS"`   // 系统操作系统
}
