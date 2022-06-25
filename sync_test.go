// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
//
// DejaVu is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//         http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package dejavu

import (
	"os"
	"testing"
)

func TestSync(t *testing.T) {
	repo, _ := initIndex(t)

	userId := os.Getenv("dejavu_sync_user")
	token := os.Getenv("dejavu_sync_token")
	if "" == token || "" == userId {
		return
	}

	err := repo.Sync("test", userId, token, "", "http://127.0.0.1:64388", nil)
	if nil != err {
		t.Fatalf("sync failed: %s", err)
		return
	}
}
