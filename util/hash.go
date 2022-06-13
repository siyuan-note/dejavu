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
