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

type Index struct {
	Hash    string   `json:"hash"`
	Parent  string   `json:"parent"`  // 指向上一个索引
	Message string   `json:"message"` // 索引备注
	Created int64    `json:"created"` // 索引时间
	Files   []string `json:"files"`   // 文件列表
}

func (c *Index) ID() string {
	if "" != c.Hash {
		return c.Hash
	}
	c.Hash = RandHash()
	return c.Hash
}
