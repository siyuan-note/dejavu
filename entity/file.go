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
	"bytes"
	"strconv"

	"github.com/siyuan-note/dejavu/util"
)

type File struct {
	ID      string   `json:"id"`      // Hash
	Path    string   `json:"path"`    // 文件路径
	Size    int64    `json:"size"`    // 文件大小
	Updated int64    `json:"updated"` // 最后更新时间
	Chunks  []string `json:"chunks"`  // 文件分块列表
}

func NewFile(path string, size int64, updated int64) (ret *File) {
	ret = &File{
		Path:    path,
		Size:    size,
		Updated: updated,
	}
	buf := bytes.Buffer{}
	buf.WriteString(ret.Path)
	buf.WriteString(strconv.FormatInt(ret.Size, 10))
	buf.WriteString(strconv.FormatInt(ret.Updated, 10))
	ret.ID = util.Hash(buf.Bytes())
	return
}
