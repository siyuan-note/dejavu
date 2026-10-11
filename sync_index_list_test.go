package dejavu

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/siyuan-note/dejavu/cloud"
)

func TestCloudIndexListObservationPreservesLatestFallback(t *testing.T) {
	root := t.TempDir()
	dataPath := filepath.Join(root, "data")
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataPath, "doc.txt"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}
	var latestID string
	var fail atomic.Bool
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/refs/latest"):
			_, _ = fmt.Fprint(writer, latestID)
		case strings.HasSuffix(request.URL.Path, "/listRepoObjects"):
			calls.Add(1)
			writer.Header().Set("Content-Type", "application/json")
			if fail.Load() {
				writer.WriteHeader(http.StatusUnauthorized)
			} else {
				_, _ = fmt.Fprint(writer, `{"code":0,"msg":"","data":{"objects":[]}}`)
			}
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	results := make(chan error, 3)
	repo, err := NewRepoWithOptions(Options{
		DataPath: dataPath, RepoPath: filepath.Join(root, "repo"), HistoryPath: filepath.Join(root, "history"), TempPath: filepath.Join(root, "temp"),
		DeviceID: "device", DeviceName: "Device", DeviceOS: "windows", AESKey: []byte("0123456789abcdef0123456789abcdef"),
		Cloud: cloud.NewSiYuan(&cloud.BaseCloud{Conf: &cloud.Conf{Dir: "main", UserID: "0", AvailableSize: 1024 * 1024,
			Endpoint: server.URL + "/", Server: server.URL}}),
		OnCloudIndexList: func(err error) { results <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	local, err := repo.Index("initial", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	latestID = local.ID
	for _, denied := range []bool{false, true, false} {
		fail.Store(denied)
		latest, latestErr := repo.GetCloudLatestFast(nil)
		if latestErr != nil || latest.ID != local.ID {
			t.Fatalf("listing failure changed latest-index fallback: %v, %v", latest, latestErr)
		}
		select {
		case observed := <-results:
			if (observed != nil) != denied {
				t.Fatalf("listing result was not reported: denied=%v, error=%v", denied, observed)
			}
			if denied && !errors.Is(observed, cloud.ErrCloudAuthFailed) {
				t.Fatalf("listing error cause was lost: %v", observed)
			}
		default:
			t.Fatal("listing result was not observed before successful latest read returned")
		}
	}
	if calls.Load() != 3 {
		t.Fatalf("observer added requests: %d", calls.Load())
	}
}
