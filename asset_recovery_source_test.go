package dejavu

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/siyuan-note/dejavu/cloud"
)

func TestAssetDownloadsRecoverDirectedSourceSwitch(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "current"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			a := newAssetTestRepo(t, base, filepath.Join(base, "A"), "local", true)
			if err := a.ConfigureSyncSource("A"); err != nil {
				t.Fatal(err)
			}
			writeAssetTestFile(t, a, "/local.txt", "local", 21)
			syncAssetTestRepo(t, a)
			b := newAssetTestRepo(t, base, filepath.Join(base, "B"), "remote", false)
			writeAssetTestFile(t, b, "/remote.txt", "remote", 22)
			syncAssetTestRepo(t, b)
			a.cloud = cloud.NewLocal(&cloud.BaseCloud{Conf: &cloud.Conf{Dir: "main", RepoPath: a.Path,
				AvailableSize: 1 << 30, Local: &cloud.ConfLocal{Endpoint: filepath.Join(base, "B")}}})
			if err := a.ConfigureSyncSource("B"); err != nil {
				t.Fatal(err)
			}
			if err := a.ConfigureAssetDownloads(true, a.assetDownloads.path, "scope-B"); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("transient preflight failure")
			a.SetCheckoutHooks(CheckoutHooks{Preflight: func(*CheckoutPlan) error { return injected }})
			if _, _, err := a.SyncDownload(nil); !errors.Is(err, injected) {
				t.Fatalf("expected injected failure: %v", err)
			}
			if a.assetDownloads.state.Pending == nil || a.assetDownloads.state.Pending.Source != "B" {
				t.Fatal("pending journal did not retain the selected source")
			}
			if legacy {
				a.assetDownloads.state.Pending.Source = ""
				if err := a.saveAssetState(); err != nil {
					t.Fatal(err)
				}
			}
			reopened, err := NewRepo(a.DataPath, a.Path, a.HistoryPath, a.TempPath,
				a.DeviceID, a.DeviceName, a.DeviceOS, a.store.AesKey, nil, a.cloud)
			if err != nil {
				t.Fatal(err)
			}
			if err = reopened.ConfigureAssetDownloads(true, a.assetDownloads.path, "scope-B"); err != nil {
				t.Fatal(err)
			}
			if !legacy {
				before, readErr := os.ReadFile(filepath.Join(a.Path, "refs", "latest-sync-source"))
				if readErr != nil {
					t.Fatal(readErr)
				}
				if _, _, err = reopened.RecoverAssetDownloads(nil); !errors.Is(err, ErrSyncSourceChanged) {
					t.Fatalf("recovery without the journal source was accepted: %v", err)
				}
				after, readErr := os.ReadFile(filepath.Join(a.Path, "refs", "latest-sync-source"))
				if readErr != nil || !bytes.Equal(before, after) || reopened.assetDownloads.state.Pending == nil {
					t.Fatal("rejected recovery changed binding or discarded its journal")
				}
				if _, err = os.Stat(a.absPath("/remote.txt")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("rejected recovery changed files: %v", err)
				}
			}
			if err = reopened.ConfigureSyncSource("B"); err != nil {
				t.Fatal(err)
			}
			id, _, err := reopened.RecoverAssetDownloads(nil)
			if err != nil {
				t.Fatalf("authorized source switch could not recover: %v", err)
			}
			if err = reopened.AcknowledgeAssetDownloadChanges(id); err != nil {
				t.Fatal(err)
			}
			if changed, sourceErr := reopened.SyncSourceChanged(); sourceErr != nil || changed {
				t.Fatalf("recovered source was not established: %t %v", changed, sourceErr)
			}
			if data, readErr := os.ReadFile(a.absPath("/remote.txt")); readErr != nil || string(data) != "remote" {
				t.Fatalf("recovery lost remote content: %q %v", data, readErr)
			}
			if _, err = os.Stat(a.absPath("/local.txt")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("directed recovery retained removed content: %v", err)
			}
			if _, _, err = reopened.Sync(nil); err != nil {
				t.Fatalf("completed recovery blocked the next sync: %v", err)
			}
		})
	}
}

func TestAssetDownloadsSwitchToEmptySource(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "A"), "local", true)
	if err := repo.ConfigureSyncSource("A"); err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, repo, "/local.txt", "local", 21)
	syncAssetTestRepo(t, repo)
	repo.cloud = cloud.NewLocal(&cloud.BaseCloud{Conf: &cloud.Conf{Dir: "main", RepoPath: repo.Path,
		AvailableSize: 1 << 30, Local: &cloud.ConfLocal{Endpoint: filepath.Join(base, "B")}}})
	if err := repo.ConfigureSyncSource("B"); err != nil {
		t.Fatal(err)
	}
	if err := repo.ConfigureAssetDownloads(true, repo.assetDownloads.path, "scope-B"); err != nil {
		t.Fatal(err)
	}
	syncAssetTestRepo(t, repo)
	if changed, err := repo.SyncSourceChanged(); err != nil || changed {
		t.Fatalf("empty source was not established: %t %v", changed, err)
	}
	if data, err := os.ReadFile(repo.absPath("/local.txt")); err != nil || string(data) != "local" {
		t.Fatalf("empty source lost local content: %q %v", data, err)
	}
}

func TestAssetDownloadsHydrationFailurePreservesSyncSource(t *testing.T) {
	for _, mode := range []string{"merge", "matching-auto", "matching-download"} {
		t.Run(mode, func(t *testing.T) {
			base := t.TempDir()
			remote := filepath.Join(base, "cloud")
			full := newAssetTestRepo(t, base, remote, "full", false)
			writeAssetTestFile(t, full, "/assets/asset.bin", "resource", 30)
			syncAssetTestRepo(t, full)
			partial := newAssetTestRepo(t, base, remote, "partial", true)
			if err := partial.ConfigureSyncSource("A"); err != nil {
				t.Fatal(err)
			}
			syncAssetTestRepo(t, partial)
			previousBase := partial.latestSync()
			writeAssetTestFile(t, full, "/remote.txt", "remote", 33)
			syncAssetTestRepo(t, full)
			if mode == "merge" {
				writeAssetTestFile(t, partial, "/local.txt", "local", 32)
				if _, err := partial.Index("local", true, nil); err != nil {
					t.Fatal(err)
				}
			} else {
				syncAssetTestRepo(t, partial)
				if err := partial.UpdateLatestSync(previousBase); err != nil {
					t.Fatal(err)
				}
				if err := partial.writeSyncSource(); err != nil {
					t.Fatal(err)
				}
			}
			if err := partial.ConfigureAssetDownloads(false, partial.assetDownloads.path, "test-scope"); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("temporary asset hydration error")
			partial.SetCheckoutHooks(CheckoutHooks{Preflight: func(plan *CheckoutPlan) error {
				for _, file := range plan.Upserts {
					if IsAssetDownloadPath(file.Path) {
						return injected
					}
				}
				return nil
			}})
			var err error
			if mode == "matching-download" {
				_, _, err = partial.SyncDownload(nil)
			} else {
				_, _, err = partial.Sync(nil)
			}
			if !errors.Is(err, injected) {
				t.Fatalf("expected hydration failure: %v", err)
			}
			if partial.assetDownloads.state.Pending != nil {
				t.Fatal("document merge did not complete before hydration")
			}
			if changed, sourceErr := partial.SyncSourceChanged(); sourceErr != nil || changed {
				t.Fatalf("hydration failure left the committed source mismatched: %t %v", changed, sourceErr)
			}
			partial.SetCheckoutHooks(CheckoutHooks{})
			if _, _, err = partial.RecoverAssetDownloads(nil); err != nil {
				t.Fatal(err)
			}
			syncAssetTestRepo(t, partial)
			if data, readErr := os.ReadFile(partial.absPath("/assets/asset.bin")); readErr != nil || string(data) != "resource" {
				t.Fatalf("retry did not hydrate the resource: %q %v", data, readErr)
			}
		})
	}
}
