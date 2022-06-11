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

	"github.com/siyuan-note/encryption"
)

func TestTag(t *testing.T) {
	clearTestdata(t)

	aesKey, err := encryption.KDF(testRepoPassword)
	if nil != err {
		return
	}

	repo, err := NewRepo(testDataPath, testRepoPath, aesKey)
	if nil != err {
		t.Fatalf("new repo failed: %s", err)
		return
	}
	index, err := repo.Commit()
	if nil != err {
		t.Fatalf("commit failed: %s", err)
		return
	}
	t.Logf("commit: %s", index.Hash)

	err = repo.AddTag(index.Hash, "tag1")
	if nil != err {
		t.Fatalf("add tag failed: %s", err)
		return
	}

	id, err := repo.GetTag("tag1")
	if id != index.Hash {
		t.Fatalf("get tag failed: %s", err)
		return
	}
}
