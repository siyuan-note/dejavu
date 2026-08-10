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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/siyuan-note/dejavu/cloud"
)

type countingLocalCloud struct {
	*cloud.Local
	lockUploads    atomic.Int32
	indexDownloads atomic.Int32
}

func (c *countingLocalCloud) UploadObject(filePath string, overwrite bool) (length int64, err error) {
	if "lock-sync" == filePath {
		c.lockUploads.Add(1)
	}
	return c.Local.UploadObject(filePath, overwrite)
}

func (c *countingLocalCloud) DownloadObject(filePath string) (data []byte, err error) {
	if strings.HasPrefix(filePath, "indexes/") {
		c.indexDownloads.Add(1)
	}
	return c.Local.DownloadObject(filePath)
}

func TestSync(t *testing.T) {
	t.Skip("需要本地思源云服务")

	repo, _ := initIndex(t)

	userId := "0"
	token := ""

	repo.cloud = &cloud.SiYuan{BaseCloud: &cloud.BaseCloud{Conf: &cloud.Conf{
		Dir:           "test",
		UserID:        userId,
		AvailableSize: 1024 * 1024 * 1024 * 8,
		Token:         token,
		Server:        "http://127.0.0.1:64388",
	}}}

	mergeResult, trafficStat, err := repo.Sync(nil)
	if nil != err {
		t.Fatalf("sync failed: %s", err)
		return
	}
	_ = mergeResult
	_ = trafficStat
}

func TestSyncCloudLockFastPath(t *testing.T) {
	tempDir := t.TempDir()
	dataPath := filepath.Join(tempDir, "data")
	repoPath := filepath.Join(tempDir, "repo")
	historyPath := filepath.Join(tempDir, "history")
	tempPath := filepath.Join(tempDir, "temp")
	cloudPath := filepath.Join(tempDir, "cloud")
	if err := os.MkdirAll(dataPath, 0755); nil != err {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataPath, "doc.txt"), []byte("data"), 0644); nil != err {
		t.Fatal(err)
	}

	localCloud := &countingLocalCloud{Local: cloud.NewLocal(&cloud.BaseCloud{Conf: &cloud.Conf{
		Dir:           "main",
		RepoPath:      repoPath,
		AvailableSize: 1024 * 1024 * 1024,
		Local:         &cloud.ConfLocal{Endpoint: cloudPath},
	}})}
	repo, err := NewRepo(dataPath, repoPath, historyPath, tempPath, "device", "Device", "windows",
		[]byte("0123456789abcdef0123456789abcdef"), nil, localCloud)
	if nil != err {
		t.Fatal(err)
	}
	if _, err = repo.Index("Initial index", false, map[string]interface{}{}); nil != err {
		t.Fatal(err)
	}
	if _, _, err = repo.Sync(map[string]interface{}{}); nil != err {
		t.Fatal(err)
	}

	localCloud.lockUploads.Store(0)
	if _, _, err = repo.Sync(map[string]interface{}{}); nil != err {
		t.Fatal(err)
	}
	if 0 != localCloud.lockUploads.Load() {
		t.Fatalf("unchanged sync uploaded cloud lock [%d] times", localCloud.lockUploads.Load())
	}

	if _, _, err = repo.Sync(map[string]interface{}{"skipCloudPreflight": true}); nil != err {
		t.Fatal(err)
	}
	if 1 != localCloud.lockUploads.Load() {
		t.Fatalf("prepared sync uploaded cloud lock [%d] times", localCloud.lockUploads.Load())
	}
}

func TestGetCloudLatestFastPath(t *testing.T) {
	tempDir := t.TempDir()
	dataPath := filepath.Join(tempDir, "data")
	repoPath := filepath.Join(tempDir, "repo")
	historyPath := filepath.Join(tempDir, "history")
	tempPath := filepath.Join(tempDir, "temp")
	cloudPath := filepath.Join(tempDir, "cloud")
	if err := os.MkdirAll(dataPath, 0755); nil != err {
		t.Fatal(err)
	}
	docPath := filepath.Join(dataPath, "doc.txt")
	if err := os.WriteFile(docPath, []byte("data"), 0644); nil != err {
		t.Fatal(err)
	}

	localCloud := &countingLocalCloud{Local: cloud.NewLocal(&cloud.BaseCloud{Conf: &cloud.Conf{
		Dir:           "main",
		RepoPath:      repoPath,
		AvailableSize: 1024 * 1024 * 1024,
		Local:         &cloud.ConfLocal{Endpoint: cloudPath},
	}})}
	repo, err := NewRepo(dataPath, repoPath, historyPath, tempPath, "device", "Device", "windows",
		[]byte("0123456789abcdef0123456789abcdef"), nil, localCloud)
	if nil != err {
		t.Fatal(err)
	}
	if _, err = repo.Index("Initial index", false, map[string]interface{}{}); nil != err {
		t.Fatal(err)
	}
	if _, _, err = repo.Sync(map[string]interface{}{}); nil != err {
		t.Fatal(err)
	}

	localCloud.indexDownloads.Store(0)
	cloudLatest, err := repo.GetCloudLatestFast(map[string]interface{}{})
	if nil != err {
		t.Fatal(err)
	}
	localLatest, err := repo.Latest()
	if nil != err {
		t.Fatal(err)
	}
	if cloudLatest.ID != localLatest.ID {
		t.Fatalf("cloud latest [%s] does not match local latest [%s]", cloudLatest.ID, localLatest.ID)
	}
	if 0 != localCloud.indexDownloads.Load() {
		t.Fatalf("unchanged fast path downloaded cloud index [%d] times", localCloud.indexDownloads.Load())
	}

	if _, err = repo.GetCloudLatest(map[string]interface{}{}); nil != err {
		t.Fatal(err)
	}
	if 1 != localCloud.indexDownloads.Load() {
		t.Fatalf("full validation downloaded cloud index [%d] times", localCloud.indexDownloads.Load())
	}

	if err = os.WriteFile(filepath.Join(dataPath, "changed.txt"), []byte("changed"), 0644); nil != err {
		t.Fatal(err)
	}
	if _, err = repo.Index("Changed index", false, map[string]interface{}{}); nil != err {
		t.Fatal(err)
	}
	localCloud.indexDownloads.Store(0)
	if _, err = repo.GetCloudLatestFast(map[string]interface{}{}); nil != err {
		t.Fatal(err)
	}
	if 1 != localCloud.indexDownloads.Load() {
		t.Fatalf("changed fast path downloaded cloud index [%d] times", localCloud.indexDownloads.Load())
	}
}

func TestGetCloudLatestFastPathReadsSiYuanRefsConcurrently(t *testing.T) {
	tempDir := t.TempDir()
	dataPath := filepath.Join(tempDir, "data")
	repoPath := filepath.Join(tempDir, "repo")
	historyPath := filepath.Join(tempDir, "history")
	tempPath := filepath.Join(tempDir, "temp")
	if err := os.MkdirAll(dataPath, 0755); nil != err {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataPath, "doc.txt"), []byte("data"), 0644); nil != err {
		t.Fatal(err)
	}

	var latestID string
	refStarted := make(chan struct{})
	listStarted := make(chan struct{})
	var refOnce, listOnce sync.Once
	requestsOverlapped := atomic.Bool{}
	indexDownloads := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/refs/latest"):
			refOnce.Do(func() { close(refStarted) })
			select {
			case <-listStarted:
				requestsOverlapped.Store(true)
			case <-time.After(2 * time.Second):
			}
			_, _ = writer.Write([]byte(latestID))
		case strings.HasSuffix(request.URL.Path, "/listRepoObjects"):
			listOnce.Do(func() { close(listStarted) })
			select {
			case <-refStarted:
				requestsOverlapped.Store(true)
			case <-time.After(2 * time.Second):
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(writer, `{"code":0,"msg":"","data":{"objects":[{"Path":"latest-1-%s","Size":40}]}}`, latestID)
		case strings.Contains(request.URL.Path, "/indexes/"):
			indexDownloads.Add(1)
			http.Error(writer, "unexpected index download", http.StatusInternalServerError)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	siyuanCloud := cloud.NewSiYuan(&cloud.BaseCloud{Conf: &cloud.Conf{
		Dir:           "main",
		UserID:        "0",
		AvailableSize: 1024 * 1024 * 1024,
		Endpoint:      server.URL + "/",
		Server:        server.URL,
	}})
	repo, err := NewRepo(dataPath, repoPath, historyPath, tempPath, "device", "Device", "windows",
		[]byte("0123456789abcdef0123456789abcdef"), nil, siyuanCloud)
	if nil != err {
		t.Fatal(err)
	}
	localLatest, err := repo.Index("Initial index", false, map[string]interface{}{})
	if nil != err {
		t.Fatal(err)
	}
	latestID = localLatest.ID

	cloudLatest, err := repo.GetCloudLatestFast(map[string]interface{}{})
	if nil != err {
		t.Fatal(err)
	}
	if cloudLatest.ID != localLatest.ID {
		t.Fatalf("cloud latest [%s] does not match local latest [%s]", cloudLatest.ID, localLatest.ID)
	}
	if !requestsOverlapped.Load() {
		t.Fatal("refs/latest and sequence refs requests did not overlap")
	}
	if 0 != indexDownloads.Load() {
		t.Fatalf("unchanged fast path downloaded cloud index [%d] times", indexDownloads.Load())
	}
}
