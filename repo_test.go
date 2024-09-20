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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/encryption"
	"github.com/siyuan-note/eventbus"
	"golang.org/x/exp/rand"
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

var (
	deviceID      = "device-id-0"
	deviceName, _ = os.Hostname()
	deviceOS      = runtime.GOOS
)

func TestIndexEmpty(t *testing.T) {
	clearTestdata(t)
	subscribeEvents(t)

	aesKey, err := encryption.KDF(testRepoPassword, testRepoPasswordSalt)
	if nil != err {
		return
	}

	testEmptyDataPath := "testdata/empty-data"
	if err = os.MkdirAll(testEmptyDataPath, 0755); nil != err {
		t.Fatalf("mkdir failed: %s", err)
		return
	}
	repo, err := NewRepo(testEmptyDataPath, testRepoPath, testHistoryPath, testTempPath, deviceID, deviceName, deviceOS, aesKey, ignoreLines(), nil)
	if nil != err {
		t.Fatalf("new repo failed: %s", err)
		return
	}
	_, err = repo.Index("Index 1", map[string]interface{}{})
	if !errors.Is(err, ErrEmptyIndex) {
		t.Fatalf("should be empty index")
		return
	}
}

func TestPurge(t *testing.T) {
	clearTestdata(t)
	subscribeEvents(t)

	repo, _ := initIndex(t)
	stat, err := repo.Purge()
	if nil != err {
		t.Fatalf("purge failed: %s", err)
		return
	}

	t.Logf("purge stat: %#v", stat)
}

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
	repo, err = NewRepo(testDataCheckoutPath, testRepoPath, testHistoryPath, testTempPath, deviceID, deviceName, deviceOS, aesKey, ignoreLines(), nil)
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

func TestPurgeV2(t *testing.T) {
	clearTestdata(t)
	firstFile := "abc"
	secFile := "def"
	//添加文件
	addFile(t, firstFile)
	repo, initIndex := initIndex(t)

	_ = initIndex

	//遍历 repo文件夹，并收集object
	objIDs := map[string]bool{}

	collectObjErr := repo.store.storeDirWalk("objects", func(_ int, entry fs.DirEntry, _ string) (cbErr error) {
		if !entry.IsDir() {
			return
		}
		dirName := entry.Name()
		dir := filepath.Join("objects", dirName)
		cbErr = repo.store.storeDirWalk(dir, func(_ int, entry fs.DirEntry, _ string) (cbErr error) {
			id := dirName + entry.Name()
			objIDs[id] = true
			return
		})
		return
	})
	if nil != collectObjErr {
		t.Fatalf("PurgeV2 collect object failed: %v", collectObjErr)
		return
	}

	if len(objIDs) <= 1 {
		t.Fatalf("put obj error")
	}

	// 放入新文件
	addFile(t, secFile)

	newIndex, err := repo.index("new obj", map[string]interface{}{})
	if err != nil {
		t.Fatalf("create new snapshot fail")
		return
	}

	//测试是否会删除最新快照
	err = repo.DeleteIndex(newIndex.ID)
	if err == nil {
		t.Fatalf("delete latest index")
		return
	}

	//删除文件
	err = os.RemoveAll(filepath.Join(testDataPath, secFile))
	if err != nil {
		t.Fatalf("delete new file failed")
		return
	}
	err = os.RemoveAll(filepath.Join(testDataPath, firstFile))
	if err != nil {
		t.Fatalf("delete new file failed")
		return
	}

	//创建新快照后删除之前的快照
	testIndex, err := repo.index("an another snapshot", map[string]interface{}{})
	if err != nil {
		t.Fatalf("create new snapshot fail %s", testIndex.ID)
		return
	}

	//测试是否会删除最新快照
	err = repo.DeleteIndex(newIndex.ID)
	if err != nil {
		t.Fatalf("delete index failed")
		return
	}

	_, err = repo.PurgeV2()
	if err != nil {
		t.Fatalf("clean repo fail!")
	}

	// 检查对象是否清理干净
	newObjIDs := map[string]bool{}
	repo.store.storeDirWalk("objects", func(_ int, entry fs.DirEntry, _ string) (cbErr error) {
		if !entry.IsDir() {
			return
		}
		dirName := entry.Name()
		dir := filepath.Join("objects", dirName)
		cbErr = repo.store.storeDirWalk(dir, func(_ int, entry fs.DirEntry, _ string) (cbErr error) {
			id := dirName + entry.Name()
			newObjIDs[id] = true
			return
		})
		return
	})
	for oldID := range objIDs {
		if newObjIDs[oldID] != true {
			t.Fatalf("[%s] should not have been deleted in purge", oldID)
		}
	}
	for newID := range newObjIDs {
		if objIDs[newID] != true {
			t.Fatalf("[%s] should be delete in purge", newID)
		}
	}
}

func addFile(t *testing.T, path string) {
	// 放入新文件
	content := []byte(fmt.Sprintf("hello dejavu %d", rand.Int63()))
	err := os.WriteFile(filepath.Join(testDataPath, path), content, 0644)
	if err != nil {
		t.Fatalf("put new file error")
		return
	}
}

func clearTestdata(t *testing.T) {
	err := os.RemoveAll(testRepoPath)
	if nil != err {
		t.Fatalf("remove failed: %s", err)
		return
	}

	err = os.RemoveAll(testDataCheckoutPath)
	if nil != err {
		t.Fatalf("remove failed: %s", err)
		return
	}
}

func subscribeEvents(t *testing.T) {
	eventbus.Subscribe(eventbus.EvtIndexBeforeWalkData, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", eventbus.EvtIndexBeforeWalkData, path)
	})
	eventbus.Subscribe(eventbus.EvtIndexWalkData, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", eventbus.EvtIndexWalkData, path)
	})
	eventbus.Subscribe(eventbus.EvtIndexBeforeGetLatestFiles, func(context map[string]interface{}, total int) {
		t.Logf("[%s]: [%v/%v]", eventbus.EvtIndexBeforeGetLatestFiles, 0, total)
	})
	eventbus.Subscribe(eventbus.EvtIndexGetLatestFile, func(context map[string]interface{}, count int, total int) {
		t.Logf("[%s]: [%v/%v]", eventbus.EvtIndexGetLatestFile, count, total)
	})
	eventbus.Subscribe(eventbus.EvtIndexUpsertFiles, func(context map[string]interface{}, total int) {
		t.Logf("[%s]: [%v/%v]", eventbus.EvtIndexUpsertFiles, 0, total)
	})
	eventbus.Subscribe(eventbus.EvtIndexUpsertFile, func(context map[string]interface{}, count int, total int) {
		t.Logf("[%s]: [%v/%v]", eventbus.EvtIndexUpsertFile, count, total)
	})

	eventbus.Subscribe(eventbus.EvtCheckoutBeforeWalkData, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", eventbus.EvtCheckoutBeforeWalkData, path)
	})
	eventbus.Subscribe(eventbus.EvtCheckoutWalkData, func(context map[string]interface{}, path string) {
		t.Logf("[%s]: [%s]", eventbus.EvtCheckoutWalkData, path)
	})
	eventbus.Subscribe(eventbus.EvtCheckoutUpsertFiles, func(context map[string]interface{}, total int) {
		t.Logf("[%s]: [%d/%d]", eventbus.EvtCheckoutUpsertFiles, 0, total)
	})
	eventbus.Subscribe(eventbus.EvtCheckoutUpsertFile, func(context map[string]interface{}, count, total int) {
		t.Logf("[%s]: [%d/%d]", eventbus.EvtCheckoutUpsertFile, count, total)
	})
	eventbus.Subscribe(eventbus.EvtCheckoutRemoveFiles, func(context map[string]interface{}, total int) {
		t.Logf("[%s]: [%d/%d]", eventbus.EvtCheckoutRemoveFiles, 0, total)
	})
	eventbus.Subscribe(eventbus.EvtCheckoutRemoveFile, func(context map[string]interface{}, count, total int) {
		t.Logf("[%s]: [%d/%d]", eventbus.EvtCheckoutRemoveFile, count, total)
	})
}

func initIndex(t *testing.T) (repo *Repo, index *entity.Index) {
	aesKey, err := encryption.KDF(testRepoPassword, testRepoPasswordSalt)
	if nil != err {
		return
	}

	repo, err = NewRepo(testDataPath, testRepoPath, testHistoryPath, testTempPath, deviceID, deviceName, deviceOS, aesKey, ignoreLines(), nil)
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
