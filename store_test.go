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
	"testing"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
	"github.com/siyuan-note/encryption"
)

func TestPutGet(t *testing.T) {
	clearTestdata(t)

	aesKey, err := encryption.KDF(testRepoPassword, testRepoPasswordSalt)
	if nil != err {
		t.Fatalf("kdf failed: %s", err)
		return
	}

	store, err := NewStore(testRepoPath, aesKey)
	if nil != err {
		t.Fatalf("new store failed: %s", err)
		return
	}

	data := []byte("Hello!")
	chunk := &entity.Chunk{ID: util.Hash(data), Data: data}
	err = store.PutChunk(chunk)
	if nil != err {
		t.Fatalf("put failed: %s", err)
		return
	}

	chunk, err = store.GetChunk(chunk.ID)
	if nil != err {
		t.Fatalf("get failed: %s", err)
		return
	}
	if 0 != bytes.Compare(chunk.Data, data) {
		t.Fatalf("data not match")
		return
	}

	err = store.Remove(chunk.ID)
	if nil != err {
		t.Fatalf("remove failed: %s", err)
		return
	}

	chunk, err = store.GetChunk(chunk.ID)
	if nil != chunk {
		t.Fatalf("get should be failed")
		return
	}
}
