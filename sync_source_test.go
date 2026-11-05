package dejavu

import (
	"errors"
	"github.com/siyuan-note/dejavu/cloud"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSyncSourceSwitchRequiresDirection(t *testing.T) {
	for _, direction := range []string{"upload", "download", "empty"} {
		t.Run(direction, func(t *testing.T) {
			base := t.TempDir()
			a := newAssetTestRepo(t, base, filepath.Join(base, "A"), "local", false)
			b := newAssetTestRepo(t, base, filepath.Join(base, "B"), "remote", false)
			if direction != "empty" {
				syncAssetTestRepo(t, b)
			}
			if err := a.ConfigureSyncSource("A"); err != nil {
				t.Fatal(err)
			}
			writeAssetTestFile(t, a, "/new.txt", "must survive automatic merge", 20)
			writeAssetTestFile(t, a, "/seed.txt", "new local seed", 21)
			syncAssetTestRepo(t, a)
			bindingPath := filepath.Join(a.Path, "sync-source.json")
			before, err := os.ReadFile(bindingPath)
			if err != nil {
				t.Fatal(err)
			}
			a.cloud = cloud.NewLocal(&cloud.BaseCloud{Conf: &cloud.Conf{Dir: "main", RepoPath: a.Path, AvailableSize: 1024 * 1024 * 1024, Local: &cloud.ConfLocal{Endpoint: filepath.Join(base, "B")}}})
			if err = a.ConfigureSyncSource("B"); err != nil {
				t.Fatal(err)
			}
			_, _, err = a.Sync(nil)
			if direction == "empty" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if !errors.Is(err, ErrSyncSourceChanged) {
					t.Fatalf("unguarded merge: %v", err)
				}
				if _, err = os.Stat(a.absPath("/new.txt")); err != nil {
					t.Fatal(err)
				}
				after, _ := os.ReadFile(bindingPath)
				if string(after) != string(before) {
					t.Fatal("failed sync advanced source binding")
				}
				remote, previewErr := a.GetCloudLatest(nil)
				if previewErr != nil {
					t.Fatal(previewErr)
				}
				preview, previewErr := a.PreviewSyncSource(remote, nil)
				if previewErr != nil || preview.LocalFiles != 2 || preview.UploadFiles != 2 || preview.RemoveFiles != 1 || preview.OverwriteFiles != 1 {
					t.Fatalf("preview: %+v %v", preview, previewErr)
				}
				if _, err = a.SyncUpload(map[string]interface{}{CtxExpectedSyncCloud: "stale"}); !errors.Is(err, ErrSyncSourcePreviewChanged) {
					t.Fatalf("stale preview: %v", err)
				}
				if _, _, err = a.SyncDownload(map[string]interface{}{CtxExpectedSyncLocal: "stale"}); !errors.Is(err, ErrSyncSourcePreviewChanged) {
					t.Fatalf("stale local preview: %v", err)
				}
				if direction == "upload" {
					_, err = a.SyncUpload(nil)
				} else {
					_, _, err = a.SyncDownload(nil)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if changed, err := a.SyncSourceChanged(); err != nil || changed {
				t.Fatalf("source not established: %v %v", changed, err)
			}
			_, err = os.Stat(a.absPath("/new.txt"))
			if direction == "download" {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("download did not remove: %v", err)
				}
				seed, readErr := os.ReadFile(a.absPath("/seed.txt"))
				if readErr != nil || string(seed) != "seed" {
					t.Fatalf("download did not overwrite: %q %v", seed, readErr)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			local, _ := a.Latest()
			cloud, err := a.GetCloudLatest(nil)
			if err != nil || cloud.ID != local.ID {
				t.Fatalf("different latest: %v", err)
			}
		})
	}
}

func TestSyncSourceLegacyAndInterruptedBinding(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "local", false)
	syncAssetTestRepo(t, repo)
	if err := repo.ConfigureSyncSource("old"); err != nil {
		t.Fatal(err)
	}
	if err := repo.BindLegacySyncSource(); err != nil {
		t.Fatal(err)
	}
	if err := repo.ConfigureSyncSource("new"); err != nil {
		t.Fatal(err)
	}
	if changed, err := repo.SyncSourceChanged(); err != nil || !changed {
		t.Fatalf("legacy source lost: %v %v", changed, err)
	}
	if err := repo.ConfigureSyncSource("old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo.Path, "refs", "latest-sync"), []byte(strings.Repeat("a", 40)), 0600); err != nil {
		t.Fatal(err)
	}
	if changed, err := repo.SyncSourceChanged(); err != nil || !changed {
		t.Fatalf("interrupted pair accepted: %v %v", changed, err)
	}
	path := filepath.Join(repo.Path, "sync-source.json")
	unknown := []byte(`{"version":2,"source":"old","baseline":"unknown"}`)
	if err := os.WriteFile(path, unknown, 0600); err != nil {
		t.Fatal(err)
	}
	if err := repo.ConfigureSyncSource("new"); err == nil {
		t.Fatal("unknown version accepted")
	}
	if err := repo.BindLegacySyncSource(); err == nil {
		t.Fatal("unknown version overwritten")
	}
	got, _ := os.ReadFile(path)
	if string(got) != string(unknown) {
		t.Fatal("unknown source record modified")
	}
}

type sourceUploadFailureCloud struct{ cloud.Cloud }

func (c sourceUploadFailureCloud) UploadObject(path string, overwrite bool) (int64, error) {
	if path == "refs/latest" {
		return 0, errors.New("injected source upload failure")
	}
	return c.Cloud.UploadObject(path, overwrite)
}

func TestSyncSourceFailedUploadDoesNotConfirm(t *testing.T) {
	base := t.TempDir()
	a := newAssetTestRepo(t, base, filepath.Join(base, "A"), "local", false)
	b := newAssetTestRepo(t, base, filepath.Join(base, "B"), "remote", false)
	syncAssetTestRepo(t, b)
	if err := a.ConfigureSyncSource("A"); err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, a, "/new.txt", "new local data", 30)
	syncAssetTestRepo(t, a)
	a.cloud = sourceUploadFailureCloud{cloud.NewLocal(&cloud.BaseCloud{Conf: &cloud.Conf{Dir: "main", RepoPath: a.Path, AvailableSize: 1 << 30, Local: &cloud.ConfLocal{Endpoint: filepath.Join(base, "B")}}})}
	if err := a.ConfigureSyncSource("B"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(a.Path, "sync-source.json")
	before, _ := os.ReadFile(path)
	if _, err := a.SyncUpload(nil); err == nil {
		t.Fatal("upload should fail")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("failed upload confirmed source")
	}
	if _, _, err := a.Sync(nil); !errors.Is(err, ErrSyncSourceChanged) {
		t.Fatalf("retry not guarded: %v", err)
	}
}
