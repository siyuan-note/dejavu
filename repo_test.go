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
	"path/filepath"
	"testing"

	"github.com/88250/gulu"
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

func TestIndexCheckout(t *testing.T) {
	clearTestdata(t)

	repo, index := initIndex(t)
	index2, err := repo.Index("Index 2", nil, nil)
	if nil != err {
		t.Fatalf("index failed: %s", err)
		return
	}
	if index.ID != index2.ID {
		t.Fatalf("index failed: %s", err)
		return
	}

	aesKey := repo.store.AesKey
	repo, err = NewRepo(testDataCheckoutPath, testRepoPath, aesKey)
	if nil != err {
		t.Fatalf("new repo failed: %s", err)
		return
	}
	err = repo.Checkout(index.ID, t, checkoutCallbacks)
	if nil != err {
		t.Fatalf("checkout failed: %s", err)
		return
	}

	if !gulu.File.IsExist(filepath.Join(testDataCheckoutPath, "foo")) {
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

var checkoutCallbacks = map[string]Callback{
	"walkData": func(context, arg interface{}, err error) {
		t := context.(*testing.T)
		t.Logf("checkout walkData: %+v", arg)
	},
	"upsertFile": func(context, arg interface{}, err error) {
		t := context.(*testing.T)
		t.Logf("checkout upsertFile: %+v", arg)
	},
	"removeFile": func(context, arg interface{}, err error) {
		t := context.(*testing.T)
		t.Logf("checkout removeFile: %+v", arg)
	},
}

var indexCallbacks = map[string]Callback{
	"walkData": func(context, arg interface{}, err error) {
		t := context.(*testing.T)
		t.Logf("index walkData: %+v", arg)
	},
	"getLatestFile": func(context, arg interface{}, err error) {
		t := context.(*testing.T)
		t.Logf("index getLatestFile: %+v", arg)
	},
	"upsertFile": func(context, arg interface{}, err error) {
		t := context.(*testing.T)
		t.Logf("index upsertFile: %+v", arg)
	},
}

func initIndex(t *testing.T) (repo *Repo, index *entity.Index) {
	aesKey, err := encryption.KDF(testRepoPassword, testRepoPasswordSalt)
	if nil != err {
		return
	}

	repo, err = NewRepo(testDataPath, testRepoPath, aesKey)
	if nil != err {
		t.Fatalf("new repo failed: %s", err)
		return
	}
	index, err = repo.Index("Index 1", t, indexCallbacks)
	if nil != err {
		t.Fatalf("index failed: %s", err)
		return
	}
	return
}
