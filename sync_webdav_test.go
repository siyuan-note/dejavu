package dejavu

import (
	"bytes"
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
	"github.com/siyuan-note/dejavu/entity"
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

func TestWebDAVTraversalLatestIDDoesNotOverwriteNote(t *testing.T) {
	root := t.TempDir()
	dataPath := filepath.Join(root, "data")
	cloudPath := filepath.Join(root, "cloud")
	notePath := filepath.Join(dataPath, "notebook", "document.sy")
	original := []byte(`{"ID":"20260101000000-doc001","Spec":"2","Type":"NodeDocument"}`)
	if err := os.MkdirAll(filepath.Dir(notePath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(notePath, original, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(cloudPath, "main", "siyuan", "repo", "refs"), 0755); err != nil {
		t.Fatal(err)
	}
	var escapedRequests atomic.Int32
	handler := &webdavserver.Handler{FileSystem: webdavserver.Dir(cloudPath), LockSystem: webdavserver.NewMemLS()}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/data/notebook/document.sy") {
			escapedRequests.Add(1)
		}
		handler.ServeHTTP(w, r)
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
	if _, _, err = repo.Sync(nil); err != nil {
		t.Fatalf("initial sync failed: %v", err)
	}
	escapedRequests.Store(0)

	traversalID := "..//../././././data/notebook/document.sy"
	if err = os.WriteFile(filepath.Join(cloudPath, "main", "siyuan", "repo", "refs", "latest"), []byte(traversalID), 0644); err != nil {
		t.Fatal(err)
	}
	if _, _, err = repo.SyncDownload(nil); !errors.Is(err, errInvalidIndexID) {
		t.Fatalf("sync accepted traversal latest ID: %v", err)
	}
	post, err := os.ReadFile(notePath)
	if nil != err || !bytes.Equal(post, original) {
		t.Fatalf("note was overwritten: %q, %v", post, err)
	}
	if 0 != escapedRequests.Load() {
		t.Fatalf("sync requested the traversal index path: %d", escapedRequests.Load())
	}

	if err = repo.UpdateLatest(&entity.Index{ID: traversalID}); !errors.Is(err, errInvalidIndexID) {
		t.Fatalf("update accepted traversal index ID: %v", err)
	}
	post, err = os.ReadFile(notePath)
	if nil != err || !bytes.Equal(post, original) {
		t.Fatalf("note was overwritten by update: %q, %v", post, err)
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
