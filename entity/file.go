// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
//
// DejaVu is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//         http://license.coscl.org.cn/MulanPSL2
//
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
//
// See the Mulan PSL v2 for more details.

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
