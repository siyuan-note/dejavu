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

import "github.com/siyuan-note/dejavu/util"

type Index struct {
	Hash    string   `json:"hash"`
	Parent  string   `json:"parent"`  // 指向上一个索引
	Message string   `json:"message"` // 索引备注
	Created int64    `json:"created"` // 索引时间
	Files   []string `json:"files"`   // 文件列表
	Size    int64    `json:"size"`    // 文件总大小
}

func (c *Index) ID() string {
	if "" != c.Hash {
		return c.Hash
	}
	c.Hash = util.RandHash()
	return c.Hash
}
