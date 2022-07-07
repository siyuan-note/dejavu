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
	"crypto/rand"
	"crypto/sha1"
	"fmt"

	"github.com/88250/gulu"
)

func Hash(data []byte) string {
	return fmt.Sprintf("%x", sha1.Sum(data))
}

func RandHash() string {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if nil != err {
		return Hash([]byte(gulu.Rand.String(512)))
	}
	return Hash(b)
}
