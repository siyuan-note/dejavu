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
	"github.com/siyuan-note/eventbus"
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
	subscribeEvents(t)

	repo, index := initIndex(t)
	index2, err := repo.Index("Index 2", map[string]interface{}{})
	if nil != err {
		t.Fatalf("index failed: %s", err)
		return
	}
	if index.ID != index2.ID {
		t.Fatalf("index id not match")
		return
	}

	aesKey := repo.store.AesKey
	repo, err = NewRepo(testDataCheckoutPath, testRepoPath, aesKey, ignoreLines())
	if nil != err {
		t.Fatalf("new repo failed: %s", err)
		return
	}
	_, _, err = repo.Checkout(index.ID, map[string]interface{}{})
	if nil != err {
		t.Fatalf("checkout failed: %s", err)
		return
	}

	if !gulu.File.IsExist(filepath.Join(testDataCheckoutPath, "foo")) {
		t.Fatalf("checkout failed")
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

func subscribeEvents(t *testing.T) {
	eventbus.Subscribe(EvtIndexWalkData, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", EvtIndexWalkData, path)
	})
	eventbus.Subscribe(EvtIndexGetLatestFile, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", EvtIndexGetLatestFile, path)
	})
	eventbus.Subscribe(EvtIndexUpsertFile, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", EvtIndexUpsertFile, path)
	})

	eventbus.Subscribe(EvtCheckoutWalkData, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", EvtCheckoutWalkData, path)
	})
	eventbus.Subscribe(EvtCheckoutUpsertFile, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", EvtCheckoutUpsertFile, path)
	})
	eventbus.Subscribe(EvtCheckoutRemoveFile, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", EvtCheckoutRemoveFile, path)
	})
}

func initIndex(t *testing.T) (repo *Repo, index *entity.Index) {
	aesKey, err := encryption.KDF(testRepoPassword, testRepoPasswordSalt)
	if nil != err {
		return
	}

	repo, err = NewRepo(testDataPath, testRepoPath, aesKey, ignoreLines())
	if nil != err {
		t.Fatalf("new repo failed: %s", err)
		return
	}
	index, err = repo.Index("Index 1", map[string]interface{}{})
	if nil != err {
		t.Fatalf("index failed: %s", err)
		return
	}
	return
}

func ignoreLines() []string {
	return []string{"bar"}
}
