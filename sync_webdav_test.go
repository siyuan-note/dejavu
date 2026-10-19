package dejavu

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/siyuan-note/dejavu/cloud"
	"github.com/studio-b12/gowebdav"
	webdavserver "golang.org/x/net/webdav"
)

func TestWebDAVUnreadableLatestStopsSyncAndDownload(t *testing.T) {
	root := t.TempDir()
	dataPath := filepath.Join(root, "data")
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataPath, "doc.txt"), []byte("preserve local data"), 0644); err != nil {
		t.Fatal(err)
	}
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	indexes := encoder.EncodeAll([]byte(`{"indexes":[{"id":"existing"}]}`), nil)
	var writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			writes.Add(1)
			w.WriteHeader(500)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/indexes-v2.json") {
			_, _ = w.Write(indexes)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	repoPath := filepath.Join(root, "repo")
	webdav := cloud.NewWebDAV(&cloud.BaseCloud{Conf: &cloud.Conf{Dir: "main", RepoPath: repoPath, AvailableSize: 1 << 30,
		WebDAV: &cloud.ConfWebDAV{ConcurrentReqs: 1}}}, gowebdav.NewClient(server.URL, "", ""))
	repo, err := NewRepo(dataPath, repoPath, filepath.Join(root, "history"), filepath.Join(root, "temp"),
		"device", "Device", "windows", []byte("0123456789abcdef0123456789abcdef"), nil, webdav)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repo.Index("initial", false, nil); err != nil {
		t.Fatal(err)
	}
	for _, fast := range []bool{false, true} {
		if fast {
			_, err = repo.GetCloudLatestFast(nil)
		} else {
			_, err = repo.GetCloudLatest(nil)
		}
		if !errors.Is(err, cloud.ErrCloudCheckFailed) || errors.Is(err, cloud.ErrCloudObjectNotFound) {
			t.Fatalf("download reported an empty cloud: %v", err)
		}
	}
	for range 2 {
		_, traffic, syncErr := repo.Sync(nil)
		if !errors.Is(syncErr, cloud.ErrCloudCheckFailed) || traffic.UploadChunkCount != 0 {
			t.Fatalf("sync accepted unreadable cloud or reuploaded chunks: %+v %v", traffic, syncErr)
		}
	}
	if writes.Load() != 0 {
		t.Fatalf("failed preflight changed cloud data: %d", writes.Load())
	}
	data, err := os.ReadFile(filepath.Join(dataPath, "doc.txt"))
	if err != nil || string(data) != "preserve local data" {
		t.Fatalf("failed sync changed local data: %q %v", data, err)
	}
}

func TestWebDAVRepeatedSyncDoesNotReuploadUnchangedChunks(t *testing.T) {
	root := t.TempDir()
	dataPath := filepath.Join(root, "data")
	cloudPath := filepath.Join(root, "cloud")
	for _, dir := range []string{dataPath, filepath.Join(cloudPath, "main", "siyuan", "repo", "refs")} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dataPath, "doc.txt"), []byte("normal sync"), 0644); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(&webdavserver.Handler{FileSystem: webdavserver.Dir(cloudPath), LockSystem: webdavserver.NewMemLS()})
	defer server.Close()
	repoPath := filepath.Join(root, "repo")
	webdav := cloud.NewWebDAV(&cloud.BaseCloud{Conf: &cloud.Conf{Dir: "main", RepoPath: repoPath, AvailableSize: 1 << 30,
		WebDAV: &cloud.ConfWebDAV{ConcurrentReqs: 1}}}, gowebdav.NewClient(server.URL, "", ""))
	repo, err := NewRepo(dataPath, repoPath, filepath.Join(root, "history"), filepath.Join(root, "temp"),
		"device", "Device", "windows", []byte("0123456789abcdef0123456789abcdef"), nil, webdav)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = repo.Index("initial", false, nil); err != nil {
		t.Fatal(err)
	}
	_, traffic, err := repo.Sync(nil)
	if err != nil || traffic.UploadChunkCount == 0 {
		t.Fatalf("initial upload: %+v %v", traffic, err)
	}
	_, traffic, err = repo.Sync(nil)
	if err != nil || traffic.UploadChunkCount != 0 {
		t.Fatalf("unchanged sync reuploaded chunks: %+v %v", traffic, err)
	}
	logs, _, total, err := repo.GetCloudRepoLogs(1)
	if err != nil || total != 1 || len(logs) != 1 {
		t.Fatalf("cloud snapshots: %v %d %v", logs, total, err)
	}
}
