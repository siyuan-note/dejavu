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
	"os"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/encryption"
)

const (
	testRepoPassword     = "pass"
	testRepoPasswordSalt = "salt"
	testRepoPath         = "testdata/repo"
	testDataPath         = "testdata/data"
	testDataCheckoutPath = "testdata/data-checkout"
)

func TestCommitCheckout(t *testing.T) {
	clearTestdata(t)

	repo, index := initCommit(t)
	index2, err := repo.Commit("Commit 2")
	if nil != err {
		t.Fatalf("commit failed: %s", err)
		return
	}
	if index.ID != index2.ID {
		t.Fatalf("commit failed: %s", err)
		return
	}

	aesKey := repo.store.AesKey
	repo, err = NewRepo(testDataCheckoutPath, testRepoPath, aesKey)
	if nil != err {
		t.Fatalf("new repo failed: %s", err)
		return
	}
	err = repo.Checkout(index.ID)
	if nil != err {
		t.Fatalf("checkout failed: %s", err)
		return
	}

	err = os.RemoveAll(testDataCheckoutPath)
	if nil != err {
		t.Fatalf("remove failed: %s", err)
		return
	}
	err = repo.Checkout(index.ID)
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
}

func initCommit(t *testing.T) (repo *Repo, index *entity.Index) {
	aesKey, err := encryption.KDF(testRepoPassword, testRepoPasswordSalt)
	if nil != err {
		return
	}

	repo, err = NewRepo(testDataPath, testRepoPath, aesKey)
	if nil != err {
		t.Fatalf("new repo failed: %s", err)
		return
	}
	index, err = repo.Commit("Commit 1")
	if nil != err {
		t.Fatalf("commit failed: %s", err)
		return
	}
	return
}
