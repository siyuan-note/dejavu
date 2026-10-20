package cloud

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/studio-b12/gowebdav"
	dav "golang.org/x/net/webdav"
)

type webDAVFixture struct {
	mu        sync.Mutex
	objects   map[string][]byte
	getStatus map[string]int
	getData   map[string][]byte
	reads     map[string]int
	writes    int
}

func TestWebDAVDirectoryCreation(t *testing.T) {
	for _, mode := range []string{"first failure", "405 absent", "405 existing", "file parent", "new client", "expired", "failed write", "remove"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			handler := &dav.Handler{FileSystem: dav.Dir(root), LockSystem: dav.NewMemLS()}
			var mu sync.Mutex
			var created []string
			failWrite := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if r.Method == "MKCOL" {
					created = append(created, r.URL.Path)
					if mode == "first failure" {
						w.WriteHeader(409)
						return
					}
					if mode == "405 absent" {
						w.WriteHeader(405)
						return
					}
				}
				if failWrite && r.Method == "PUT" {
					w.WriteHeader(403)
					return
				}
				handler.ServeHTTP(w, r)
			}))
			t.Cleanup(server.Close)
			newClient := func() *WebDAV {
				return NewWebDAV(&BaseCloud{Conf: &Conf{Dir: "main", WebDAV: &ConfWebDAV{}}}, gowebdav.NewClient(server.URL, "", ""))
			}
			client := newClient()
			if mode == "405 existing" || mode == "file parent" {
				if err := os.Mkdir(filepath.Join(root, "main"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "file parent" {
				if err := os.WriteFile(filepath.Join(root, "main", "siyuan"), []byte("file"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			_, err := client.UploadBytes("probe", []byte("value"), true)
			switch mode {
			case "first failure":
				var status gowebdav.StatusError
				if !errors.As(err, &status) || status.Status != 409 || len(created) != 1 || created[0] != "/main/" {
					t.Fatalf("lost first failure: %v, MKCOL %v", err, created)
				}
				return
			case "405 absent":
				if !errors.Is(err, ErrWebDAVInvalidResponse) || len(created) != 1 || len(client.dirs) != 0 {
					t.Fatalf("accepted missing collection: %v %v", err, created)
				}
				return
			case "file parent":
				if !errors.Is(err, os.ErrExist) || len(client.dirs) != 0 {
					t.Fatalf("accepted file as collection: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "405 existing" {
				return
			}
			if mode == "new client" {
				client = newClient()
			}
			if mode == "expired" {
				for key := range client.dirs {
					client.dirs[key] = time.Now().Add(-time.Second)
				}
			}
			if mode == "failed write" {
				mu.Lock()
				failWrite = true
				mu.Unlock()
				if _, err = client.UploadBytes("probe", []byte("value"), true); err == nil {
					t.Fatal("write unexpectedly succeeded")
				}
				if len(client.dirs) != 0 {
					t.Fatal("failed write retained directories")
				}
				mu.Lock()
				failWrite = false
				mu.Unlock()
			}
			if mode == "remove" {
				if err = client.RemoveObject("probe"); err != nil {
					t.Fatal(err)
				}
				if len(client.dirs) != 0 {
					t.Fatal("remove retained directories")
				}
			}
			if err = os.RemoveAll(filepath.Join(root, "main")); err != nil {
				t.Fatal(err)
			}
			if _, err = client.UploadBytes("probe", []byte("value"), true); err != nil {
				t.Fatal(err)
			}
			if data, err := os.ReadFile(filepath.Join(root, "main", "siyuan", "repo", "probe")); err != nil || string(data) != "value" {
				t.Fatalf("upload not recovered: %q %v", data, err)
			}
		})
	}
}

func TestWebDAVHTMLMetadata(t *testing.T) {
	client, f := newWebDAVFixture(t)
	for _, key := range []string{"refs/latest", "refs/tag", "lock-sync", "objects/ab/ciphertext"} {
		f.objects[path.Base(key)] = []byte(" \n<!DOCTYPE HTML><html>private response</html>")
		data, err := client.DownloadObject(key)
		if strings.HasPrefix(key, "objects/") {
			if err != nil || len(data) == 0 {
				t.Fatalf("encrypted object was inspected: %v", err)
			}
		} else if !errors.Is(err, ErrWebDAVInvalidResponse) || data != nil || strings.Contains(err.Error(), "private response") {
			t.Fatalf("wrong HTML diagnostic: %q %v", data, err)
		}
	}
	for _, status := range []int{200, 401, 403, 404, 500} {
		err := client.parseErr(&os.PathError{Op: "PROPFIND", Path: "private", Err: gowebdav.StatusError{Status: status}})
		if err == nil {
			t.Fatalf("HTTP %d swallowed", status)
		}
	}
}

func newWebDAVFixture(t *testing.T) (*WebDAV, *webDAVFixture) {
	t.Helper()
	f := &webDAVFixture{objects: map[string][]byte{}, getStatus: map[string]int{}, getData: map[string][]byte{}, reads: map[string]int{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		key := path.Base(r.URL.Path)
		switch r.Method {
		case "GET":
			f.reads[key]++
			if status := f.getStatus[key]; status != 0 {
				w.WriteHeader(status)
				return
			}
			data, ok := f.objects[key]
			if override, found := f.getData[key]; found {
				data, ok = override, true
			}
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(data)
		case "PUT":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(500)
				return
			}
			f.objects[key] = data
			f.writes++
			w.WriteHeader(201)
		case "PROPFIND":
			data, exists := f.objects[key]
			resource := ""
			if key == "refs" {
				resource = "<d:collection/>"
			} else if !exists {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(207)
			_, _ = fmt.Fprintf(w, `<?xml version="1.0"?><d:multistatus xmlns:d="DAV:"><d:response><d:href>%s</d:href><d:propstat><d:prop><d:resourcetype>%s</d:resourcetype><d:getcontentlength>%d</d:getcontentlength></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>`, r.URL.Path, resource, len(data))
		default:
			w.WriteHeader(405)
		}
	}))
	t.Cleanup(server.Close)
	return NewWebDAV(&BaseCloud{Conf: &Conf{Dir: "main", RepoPath: t.TempDir(), WebDAV: &ConfWebDAV{ConcurrentReqs: 1}}},
		gowebdav.NewClient(server.URL, "", "")), f
}

func webDAVCompressed(t *testing.T, source string) []byte {
	t.Helper()
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	return encoder.EncodeAll([]byte(source), nil)
}

func TestWebDAVIndexesReportReadFailures(t *testing.T) {
	for _, failure := range []string{"get404", "stat404", "get403", "empty", "corrupt", "none"} {
		t.Run(failure, func(t *testing.T) {
			webdav, f := newWebDAVFixture(t)
			f.objects["indexes-v2.json"] = webDAVCompressed(t, `{"indexes":[{"id":"good"},{"id":"bad"}]}`)
			f.objects["good"] = webDAVCompressed(t, `{"id":"good","files":["file"]}`)
			f.objects["bad"] = webDAVCompressed(t, `{"id":"bad"}`)
			switch failure {
			case "get404":
				f.getStatus["bad"] = 404
			case "stat404":
				delete(f.objects, "bad")
			case "get403":
				f.getStatus["bad"] = 403
			case "empty":
				f.objects["bad"] = nil
			case "corrupt":
				f.objects["bad"] = []byte("invalid compressed index")
			}
			indexes, pages, total, err := webdav.GetIndexes(1)
			if failure == "none" {
				if err != nil || len(indexes) != 2 || indexes[0].Files != nil {
					t.Fatalf("normal indexes: %v %v", indexes, err)
				}
			} else if err == nil || len(indexes) != 0 {
				t.Fatalf("unreadable index accepted: %v %v", indexes, err)
			}
			if pages != 1 || total != 2 {
				t.Fatalf("lost pagination: %d %d", pages, total)
			}
			if f.writes != 0 {
				t.Fatal("reading indexes changed cloud data")
			}
			indexes, _, _, err = webdav.GetIndexes(2)
			if err != nil || len(indexes) != 0 {
				t.Fatalf("out-of-range page is valid: %v", err)
			}
		})
	}
}

func TestWebDAVMissingLatestChecksIndexes(t *testing.T) {
	for _, mode := range []string{"missing", "empty", "populated", "corrupt", "null", "denied", "latest"} {
		t.Run(mode, func(t *testing.T) {
			webdav, f := newWebDAVFixture(t)
			wantMissing := false
			switch mode {
			case "missing":
				wantMissing = true
			case "empty":
				f.objects["indexes-v2.json"] = webDAVCompressed(t, `{"indexes":[]}`)
				wantMissing = true
			case "populated":
				f.objects["latest"] = []byte("stored but unreadable")
				f.getStatus["latest"] = 404
				f.objects["indexes-v2.json"] = webDAVCompressed(t, `{"indexes":[{"id":"existing"}]}`)
			case "corrupt":
				f.objects["indexes-v2.json"] = []byte("invalid")
			case "null":
				f.objects["indexes-v2.json"] = webDAVCompressed(t, "null")
			case "denied":
				f.getStatus["indexes-v2.json"] = 403
			case "latest":
				f.objects["latest"] = []byte("current")
			}
			data, err := webdav.DownloadObject("refs/latest")
			if mode == "latest" {
				if err != nil || string(data) != "current" || f.reads["indexes-v2.json"] != 0 {
					t.Fatalf("normal latest: %q %v", data, err)
				}
			} else if err == nil || errors.Is(err, ErrCloudObjectNotFound) != wantMissing {
				t.Fatalf("wrong missing classification: %v", err)
			}
			if f.writes != 0 {
				t.Fatal("missing latest probe changed cloud data")
			}
		})
	}
}

func TestWebDAVUploadedLatestMustBeReadable(t *testing.T) {
	for _, mode := range []string{"read404", "read500", "normal", "stale", "index"} {
		t.Run(mode, func(t *testing.T) {
			webdav, f := newWebDAVFixture(t)
			ref := "refs/latest"
			if mode == "index" {
				ref = "refs/another"
			}
			file := filepath.Join(webdav.Conf.RepoPath, filepath.FromSlash(ref))
			if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(file, []byte("current"), 0644); err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(mode, "read") {
				f.getStatus["latest"] = 404
				if mode == "read500" {
					f.getStatus["latest"] = 500
				}
			}
			if mode == "stale" {
				f.getData["latest"] = []byte("previous")
			}
			length, err := webdav.UploadObject(ref, true)
			if strings.HasPrefix(mode, "read") {
				if err == nil || errors.Is(err, ErrCloudObjectNotFound) {
					t.Fatalf("unreadable upload accepted as empty cloud: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if length != 7 || f.writes != 1 || string(f.objects[path.Base(ref)]) != "current" {
				t.Fatal("upload changed data or length")
			}
			if mode == "index" && f.reads["another"] != 0 {
				t.Fatal("extra read for unrelated uploads")
			}
		})
	}
}
