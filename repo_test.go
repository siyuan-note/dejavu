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
	testHistoryPath      = "testdata/history"
	testTempPath         = "testdata/temp"
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
	repo, err = NewRepo(testDataCheckoutPath, testRepoPath, testHistoryPath, testTempPath, aesKey, ignoreLines())
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
	eventbus.Subscribe(EvtIndexBeforeWalkData, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", EvtIndexBeforeWalkData, path)
	})
	eventbus.Subscribe(EvtIndexWalkData, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", EvtIndexWalkData, path)
	})
	eventbus.Subscribe(EvtIndexBeforeGetLatestFiles, func(context map[string]interface{}, path []string) {
		t.Logf("[%s]: [%v]", EvtIndexBeforeGetLatestFiles, path)
	})
	eventbus.Subscribe(EvtIndexGetLatestFile, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", EvtIndexGetLatestFile, path)
	})
	eventbus.Subscribe(EvtIndexUpsertFiles, func(context map[string]interface{}, files []*entity.File) {
		t.Logf("[%s]: [%v]", EvtIndexUpsertFiles, files)
	})
	eventbus.Subscribe(EvtIndexUpsertFile, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", EvtIndexUpsertFile, path)
	})

	eventbus.Subscribe(EvtCheckoutBeforeWalkData, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", EvtCheckoutBeforeWalkData, path)
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

	repo, err = NewRepo(testDataPath, testRepoPath, testHistoryPath, testTempPath, aesKey, ignoreLines())
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
