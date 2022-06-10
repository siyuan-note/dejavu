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

import (
	"bytes"
	"strconv"
)

type Commit struct {
	Hash    string   `json:"hash"`
	Parent  string   `json:"parent"`
	Message string   `json:"message"`
	Created int64    `json:"created"`
	Files   []string `json:"files"`
}

func (c *Commit) ID() string {
	if "" != c.Hash {
		return c.Hash
	}

	buf := bytes.Buffer{}
	buf.WriteString(c.Parent)
	buf.WriteString(c.Message)
	buf.WriteString(strconv.FormatInt(c.Created, 10))
	for _, f := range c.Files {
		buf.WriteString(f)
	}
	c.Hash = Hash(buf.Bytes())
	return c.Hash
}
