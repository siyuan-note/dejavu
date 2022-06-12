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

package dejavu

import (
	"testing"
)

func TestGetLogs(t *testing.T) {
	clearTestdata(t)

	repo, index := initCommit(t)
	err := repo.AddTag(index.ID, "v1.0.0")
	if nil != err {
		t.Fatalf("add tag failed: %s", err)
		return
	}

	logs, err := repo.GetLogs()
	if nil != err {
		t.Fatalf("get logs failed: %s", err)
		return
	}
	if 2 != len(logs) {
		t.Fatalf("logs length not match: %d", len(logs))
		return
	}

	for _, log := range logs {
		t.Logf("%+v", log)
	}
}
