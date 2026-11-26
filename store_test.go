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
	"errors"
	"path/filepath"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
	"github.com/siyuan-note/encryption"
)

func TestStoreObjectPathValidation(t *testing.T) {
	store := &Store{Path: t.TempDir()}
	for _, id := range []string{"", "a"} {
		if dir, file := store.AbsPath(id); dir != "" || file != "" {
			t.Fatalf("invalid ID %q returned paths %q and %q", id, dir, file)
		}
		if _, err := store.Stat(id); !errors.Is(err, errInvalidObjectID) {
			t.Fatalf("stat invalid ID %q: %v", id, err)
		}
		if err := store.Remove(id); !errors.Is(err, errInvalidObjectID) {
			t.Fatalf("remove invalid ID %q: %v", id, err)
		}
	}
	const id = "c84a0c9b5a0b093d3883fa579c5e08b4a8f16fa4"
	wantDir := filepath.Join(store.Path, "objects", "c8")
	wantFile := filepath.Join(wantDir, "4a0c9b5a0b093d3883fa579c5e08b4a8f16fa4")
	if dir, file := store.AbsPath(id); dir != wantDir || file != wantFile {
		t.Fatalf("valid ID returned paths %q and %q, want %q and %q", dir, file, wantDir, wantFile)
	}
}

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
