package dejavu

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
)

func TestCheckoutProgressCancelAndRetry(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	data := strings.Repeat("large file content", 300000)
	writeAssetTestFile(t, repo, "/assets/large.bin", data, 2)
	if _, err := repo.Index("large", false, nil); err != nil {
		t.Fatal(err)
	}
	file := assetTestFile(t, repo, "/assets/large.bin")
	if err := os.WriteFile(repo.absPath(file.Path), []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var progress []CheckoutProgress
	committed := 0
	repo.SetCheckoutHooks(CheckoutHooks{Context: ctx, Progress: func(p CheckoutProgress) {
		progress = append(progress, p)
		if p.Written > 0 {
			cancel()
		}
	}, Committed: func(*entity.File) { committed++ }})
	if err := repo.checkoutFile(file, repo.DataPath, 1, 1, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel lost: %v", err)
	}
	if len(progress) < 2 || progress[1].Written > 1024*1024 || committed != 0 {
		t.Fatalf("bad progress or commit: %v %d", progress, committed)
	}
	if content, err := os.ReadFile(repo.absPath(file.Path)); err != nil || string(content) != "old" {
		t.Fatal("cancellation changed destination")
	}
	entries, _ := os.ReadDir(filepath.Dir(repo.absPath(file.Path)))
	if len(entries) != 1 {
		t.Fatalf("temporary file leaked: %v", entries)
	}
	progress = nil
	repo.SetCheckoutHooks(CheckoutHooks{Progress: func(p CheckoutProgress) { progress = append(progress, p) }})
	if err := repo.checkoutFile(file, repo.DataPath, 1, 1, nil); err != nil {
		t.Fatal(err)
	}
	if content, err := os.ReadFile(repo.absPath(file.Path)); err != nil || !bytes.Equal(content, []byte(data)) {
		t.Fatal("retry changed content")
	}
	if progress[len(progress)-1].Written != file.Size {
		t.Fatal("missing final byte count")
	}
}

func TestCheckoutCleanupPreservesUnknownFiles(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	file := assetTestFile(t, repo, "/seed.txt")
	managed := filepath.Join(repo.DataPath, ".siyuan-checkout-"+strings.Repeat("a", 40)+"-123.tmp")
	legacy := repo.absPath(file.Path) + "abc1234.tmp"
	unknown := repo.absPath(file.Path) + "abc1235.tmp"
	for name, value := range map[string]string{managed: "managed", legacy: "see", unknown: "unrelated", filepath.Join(repo.DataPath, "user.tmp"): "keep"} {
		if err := os.WriteFile(name, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	released, err := repo.cleanupCheckoutTemps([]*entity.File{file})
	if err != nil || released != 10 {
		t.Fatalf("cleanup %d %v", released, err)
	}
	for _, name := range []string{managed, legacy} {
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Fatal("orphan retained", name)
		}
	}
	for _, name := range []string{unknown, filepath.Join(repo.DataPath, "user.tmp")} {
		if _, err := os.Stat(name); err != nil {
			t.Fatal("user file removed", name)
		}
	}
}

func TestCheckoutCancellationKeepsCompletedFilesAndSyncPoint(t *testing.T) {
	for _, onDemand := range []bool{false, true} {
		for _, cancelAfter := range []int{1, 2} {
			t.Run(fmt.Sprintf("onDemand=%t/cancelAfter=%d", onDemand, cancelAfter), func(t *testing.T) {
				base := t.TempDir()
				remote := filepath.Join(base, "cloud")
				source := newAssetTestRepo(t, base, remote, "source", false)
				syncAssetTestRepo(t, source)
				target := newAssetTestRepo(t, base, remote, "target", onDemand)
				if _, _, err := target.SyncDownload(nil); err != nil {
					t.Fatal(err)
				}
				previous := target.latestSync().ID
				writeAssetTestFile(t, source, "/one.txt", "one", 3)
				writeAssetTestFile(t, source, "/two.txt", "two", 3)
				syncAssetTestRepo(t, source)
				ctx, cancel := context.WithCancel(context.Background())
				var completed []*entity.File
				target.SetCheckoutHooks(CheckoutHooks{Context: ctx, Committed: func(file *entity.File) {
					completed = append(completed, file)
					if len(completed) == cancelAfter {
						cancel()
					}
				}})
				if _, _, err := target.SyncDownload(nil); !errors.Is(err, context.Canceled) {
					t.Fatalf("wrong cancellation: %v", err)
				}
				if len(completed) != cancelAfter || target.latestSync().ID != previous {
					t.Fatalf("lost completed files or advanced sync point: %v", completed)
				}
				if _, err := os.Stat(target.absPath(completed[0].Path)); err != nil {
					t.Fatal(err)
				}
				target.SetCheckoutHooks(CheckoutHooks{})
				if onDemand {
					id, _, err := target.RecoverAssetDownloads(nil)
					if err != nil {
						t.Fatal(err)
					}
					if err = target.AcknowledgeAssetDownloadChanges(id); err != nil {
						t.Fatal(err)
					}
				}
				if _, _, err := target.SyncDownload(nil); err != nil {
					t.Fatal(err)
				}
				for name, want := range map[string]string{"/one.txt": "one", "/two.txt": "two"} {
					if data, err := os.ReadFile(target.absPath(name)); err != nil || string(data) != want {
						t.Fatalf("retry failed: %q %v", data, err)
					}
				}
			})
		}
	}
}

func TestCheckoutPreflightFailurePreservesDestination(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	file := assetTestFile(t, repo, "/seed.txt")
	expected := errors.New("not enough space")
	repo.SetCheckoutHooks(CheckoutHooks{Before: func([]*entity.File) error { return expected }})
	if err := repo.checkoutFiles([]*entity.File{file}, nil); !errors.Is(err, expected) {
		t.Fatalf("lost preflight error: %v", err)
	}
	if data, err := os.ReadFile(repo.absPath(file.Path)); err != nil || string(data) != "seed" {
		t.Fatal("preflight changed data")
	}
}
