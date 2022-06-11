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
	"os"
	"testing"
)

const (
	testRepoPath = "testdata/repo"
	testDataPath = "testdata/data"
)

func TestCommitCheckout(t *testing.T) {
	clearTestdata(t)

	repo := NewRepo("F:\\SiYuan\\data\\", testRepoPath)
	index, err := repo.Commit()
	if nil != err {
		t.Fatalf("commit failed: %s", err)
		return
	}
	t.Logf("commit: %s", index.Hash)

	index, err = repo.Commit()
	if nil != err {
		t.Fatalf("commit failed: %s", err)
		return
	}
	t.Logf("commit: %s", index.Hash)

	repo = NewRepo(testDataPath, testRepoPath)
	err = repo.Checkout(index.Hash)
	if nil != err {
		t.Fatalf("checkout failed: %s", err)
		return
	}
}

func clearTestdata(t *testing.T) {
	err := os.RemoveAll(testRepoPath)
	if nil != err {
		t.Fatalf("remove failed: %s", err)
		return
	}
	err = os.RemoveAll(testDataPath)
	if nil != err {
		t.Fatalf("remove failed: %s", err)
		return
	}
}