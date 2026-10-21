package dejavu

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/siyuan-note/dejavu/cloud"
)

type checkoutGuardCloud struct {
	cloud.Cloud
	upload   func()
	download func(string) ([]byte, error)
}

func (backend *checkoutGuardCloud) UploadObject(path string, overwrite bool) (int64, error) {
	backend.upload()
	return backend.Cloud.UploadObject(path, overwrite)
}

func (backend *checkoutGuardCloud) UploadBytes(path string, data []byte, overwrite bool) (int64, error) {
	backend.upload()
	return backend.Cloud.UploadBytes(path, data, overwrite)
}

func (backend *checkoutGuardCloud) DownloadObject(path string) ([]byte, error) {
	if backend.download != nil {
		return backend.download(path)
	}
	return backend.Cloud.DownloadObject(path)
}

func TestSyncCheckoutGuardLeavesUploadAndNoChangeEditable(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	applies := 0
	repo.SetCheckoutHooks(CheckoutHooks{Apply: func() error { applies++; return nil }})
	syncAssetTestRepo(t, repo)
	if _, _, err := repo.Sync(nil); err != nil {
		t.Fatal(err)
	}
	if applies != 0 {
		t.Fatalf("upload/no-change started editing protection %d times", applies)
	}
}

func TestSyncCheckoutGuardRechecksEditsSavedDuringPreparation(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	source := newAssetTestRepo(t, base, remote, "source", false)
	writeAssetTestFile(t, source, "/remote.txt", "first", 2)
	syncAssetTestRepo(t, source)
	target := newAssetTestRepo(t, base, remote, "target", false)
	syncAssetTestRepo(t, target)
	writeAssetTestFile(t, source, "/remote.txt", "remote update", 3)
	syncAssetTestRepo(t, source)
	prepared, completed := false, 0
	target.SetCheckoutHooks(CheckoutHooks{Apply: func() error {
		if !prepared {
			prepared = true
			writeAssetTestFile(t, target, "/local.txt", "saved while transferring", 4)
		}
		return nil
	}, Applied: func() { completed++ }})
	if _, _, err := target.Sync(nil); err != nil {
		t.Fatal(err)
	}
	if !prepared || completed == 0 {
		t.Fatal("missing guarded application")
	}
	for p, want := range map[string]string{"/local.txt": "saved while transferring", "/remote.txt": "remote update"} {
		if data, err := os.ReadFile(target.absPath(p)); err != nil || string(data) != want {
			t.Fatalf("lost %s: %s, %v", p, data, err)
		}
	}
	if file := assetTestFile(t, target, "/local.txt"); file == nil {
		t.Fatal("saved edit was not included in merge")
	}
}

func TestSyncCheckoutGuardPreservesChangedDeletionSource(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	source := newAssetTestRepo(t, base, remote, "source", false)
	writeAssetTestFile(t, source, "/remove.txt", "first", 2)
	syncAssetTestRepo(t, source)
	target := newAssetTestRepo(t, base, remote, "target", false)
	syncAssetTestRepo(t, target)
	if err := os.Remove(source.absPath("/remove.txt")); err != nil {
		t.Fatal(err)
	}
	syncAssetTestRepo(t, source)
	prepared := false
	target.SetCheckoutHooks(CheckoutHooks{Apply: func() error {
		if !prepared {
			prepared = true
			writeAssetTestFile(t, target, "/remove.txt", "unsynced local edit", 5)
		}
		return nil
	}})
	_, _, err := target.SyncDownload(nil)
	if !errors.Is(err, ErrIndexFileChanged) {
		t.Fatalf("changed deletion source accepted: %v", err)
	}
	if data, _ := os.ReadFile(target.absPath("/remove.txt")); string(data) != "unsynced local edit" {
		t.Fatalf("lost local deletion source: %s", data)
	}
}

func TestSyncCheckoutCanceledNoChangeStopsBeforeCloudAccess(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	syncAssetTestRepo(t, repo)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	repo.SetCheckoutHooks(CheckoutHooks{Context: ctx})
	if _, _, err := repo.Sync(nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled no-change sync succeeded: %v", err)
	}
	if _, err := repo.Index("canceled", false, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled index continued: %v", err)
	}
}

func TestSyncCheckoutCancellationDuringIndexWalkKeepsPreviousSnapshot(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	syncAssetTestRepo(t, repo)
	before, err := repo.Latest()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo.SetCheckoutHooks(CheckoutHooks{Context: ctx})
	repo.pathFilter = func(info os.FileInfo, path string) (bool, error) {
		if !info.IsDir() {
			cancel()
		}
		return info.IsDir(), nil
	}
	writeAssetTestFile(t, repo, "/new.txt", "pending local edit", 10)
	if _, err = repo.Index("cancel while walking", true, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("index ignored mid-walk cancellation: %v", err)
	}
	after, err := repo.Latest()
	if err != nil || after.ID != before.ID {
		t.Fatalf("cancellation published an incomplete snapshot: %v", err)
	}
}

func TestSyncCheckoutCancellationPreservesProviderErrorIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	repo := &Repo{checkoutHooks: CheckoutHooks{Context: ctx}}
	providerErr := errors.New("upload request stopped")
	err := providerErr
	repo.normalizeCheckoutError(&err)
	if !errors.Is(err, providerErr) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost provider cause: %v", err)
	}
	err = nil
	repo.normalizeCheckoutError(&err)
	if err != nil {
		t.Fatal("a late cancellation changed a completed result")
	}
	repo.SetCheckoutHooks(CheckoutHooks{})
	err = providerErr
	repo.normalizeCheckoutError(&err)
	if err != providerErr {
		t.Fatal("an uncanceled provider error was unnecessarily wrapped")
	}
}

func TestSyncCheckoutFixesSnapshotBeforeResumingUpload(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "asset-downloads", true: "legacy"}[legacy], func(t *testing.T) {
			base := t.TempDir()
			remote := filepath.Join(base, "cloud")
			source := newAssetTestRepo(t, base, remote, "source", false)
			target := newAssetTestRepo(t, base, remote, "target", false)
			if legacy {
				source.assetDownloads, target.assetDownloads = nil, nil
			}
			writeAssetTestFile(t, source, "/remote.txt", "first", 2)
			syncAssetTestRepo(t, source)
			syncAssetTestRepo(t, target)
			writeAssetTestFile(t, source, "/remote.txt", "remote update", 3)
			syncAssetTestRepo(t, source)
			var applying atomic.Bool
			var uploads atomic.Int32
			target.cloud = &checkoutGuardCloud{Cloud: target.cloud, upload: func() {
				if applying.Load() {
					t.Error("network upload ran while local application held editing protection")
				}
				uploads.Add(1)
			}}
			prepared, completed := false, false
			target.SetCheckoutHooks(CheckoutHooks{Apply: func() error {
				applying.Store(true)
				if !prepared {
					prepared = true
					writeAssetTestFile(t, target, "/local.txt", "saved input", 4)
				}
				return nil
			}, Applied: func() {
				applying.Store(false)
				completed = true
				writeAssetTestFile(t, target, "/local.txt", "input after resuming", 5)
			}})
			if _, _, err := target.Sync(nil); err != nil {
				t.Fatal(err)
			}
			if !completed || applying.Load() || uploads.Load() == 0 {
				t.Fatal("missing transition back to background upload")
			}
			file := assetTestFile(t, target, "/local.txt")
			data, err := target.openFile(file)
			if err != nil || string(data) != "saved input" {
				t.Fatalf("snapshot included edits made after resuming: %s %v", data, err)
			}
			data, err = os.ReadFile(target.absPath("/local.txt"))
			if err != nil || string(data) != "input after resuming" {
				t.Fatalf("resumed edit was overwritten: %s %v", data, err)
			}
		})
	}
}

func TestSyncCheckoutCancelsCloudLockRetryWait(t *testing.T) {
	started := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := &Repo{checkoutHooks: CheckoutHooks{Context: ctx}, cloud: &checkoutGuardCloud{download: func(string) ([]byte, error) {
		started <- struct{}{}
		return []byte(fmt.Sprintf(`{"deviceID":"other-device","time":%d}`, time.Now().UnixMilli())), nil
	}}}
	done := make(chan error, 1)
	go func() { done <- repo.tryLockCloud("current-device", nil) }()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("lock retry lost cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation waited for the cloud lock retry interval")
	}
}
