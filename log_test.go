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
	"testing"
)

func TestGetIndexLogs(t *testing.T) {
	clearTestdata(t)

	repo, _ := initIndex(t)

	logs, pageCount, totalCount, err := repo.GetIndexLogs(1, 10)
	if nil != err {
		t.Fatalf("get index logs failed: %s", err)
		return
	}
	if 1 > len(logs) {
		t.Fatalf("logs length not match: %d", len(logs))
		return
	}

	t.Logf("page count [%d], total count [%d]", pageCount, totalCount)
	for _, log := range logs {
		t.Logf("%+v", log)
	}
}

func TestGetTagLogs(t *testing.T) {
	clearTestdata(t)

	repo, index := initIndex(t)
	err := repo.AddTag(index.ID, "v1.0.0")
	if nil != err {
		t.Fatalf("add tag failed: %s", err)
		return
	}

	err = repo.AddTag(index.ID, "v1.0.1")
	if nil != err {
		t.Fatalf("add tag failed: %s", err)
		return
	}

	logs, err := repo.GetTagLogs()
	if nil != err {
		t.Fatalf("get tag logs failed: %s", err)
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
