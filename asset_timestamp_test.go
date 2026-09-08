package dejavu

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/siyuan-note/dejavu/entity"
)

func TestAssetSyncSameContentTimestamp(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	full := newAssetTestRepo(t, base, remote, "full", false)
	path := "/snippets/config.json"
	writeAssetTestFile(t, full, path, "same", 100)
	syncAssetTestRepo(t, full)
	partial := newAssetTestRepo(t, base, remote, "partial", true)
	syncAssetTestRepo(t, partial)
	writeAssetTestFile(t, full, path, "same", 200)
	syncAssetTestRepo(t, full)
	writeAssetTestFile(t, partial, "/local.txt", "local change", 201)
	syncAssetTestRepo(t, partial)
	expected := assetTestFile(t, full, path)
	info, err := os.Stat(partial.absPath(path))
	if err != nil {
		t.Fatal(err)
	}
	if info.ModTime().UnixMilli() != expected.Updated {
		t.Fatalf("mtime not aligned: got %d, want %d", info.ModTime().UnixMilli(), expected.Updated)
	}
	for i := 0; i < 2; i++ {
		index, err := partial.Index("verify", true, nil)
		if err != nil {
			t.Fatal(err)
		}
		merge, _, err := partial.Sync(nil)
		if err != nil || merge.DataChanged() {
			t.Fatalf("sync did not converge: %+v %v", merge, err)
		}
		latest, err := partial.Latest()
		if err != nil || latest.ID != index.ID {
			t.Fatalf("sync changed stable index: %v", err)
		}
	}
}

func TestAssetApplySameContentTimestamp(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		name := "checkout"
		if recovery {
			name = "recovery"
		}
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", true)
			path := "/public/icon.png"
			writeAssetTestFile(t, repo, path, "same", 200)
			syncAssetTestRepo(t, repo)
			target, err := repo.Latest()
			if err != nil {
				t.Fatal(err)
			}
			file := assetTestFile(t, repo, path)
			writeAssetTestFile(t, repo, path, "same", 100)
			if recovery {
				repo.assetDownloads.state.Pending = &assetApply{Index: target, Base: target,
					Deferred: map[string]*entity.File{}, Before: map[string]*entity.File{path: file},
					Upserts: []*entity.File{file}}
				if err = repo.saveAssetState(); err != nil {
					t.Fatal(err)
				}
				if err = repo.ConfigureAssetDownloads(true, repo.assetDownloads.path, "test-scope"); err != nil {
					t.Fatal(err)
				}
				_, _, err = repo.RecoverAssetDownloads(nil)
			} else {
				err = repo.checkoutFile(file, repo.DataPath, 1, 1, nil, file)
			}
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(repo.absPath(path))
			if err != nil || !info.ModTime().Equal(time.UnixMilli(file.Updated)) {
				t.Fatalf("target timestamp not restored: %v %v", info, err)
			}
		})
	}
}

func TestAssetHydrateSameContentTimestamp(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	full := newAssetTestRepo(t, base, remote, "full", false)
	path := "/assets/icon.png"
	writeAssetTestFile(t, full, path, "same", 200)
	syncAssetTestRepo(t, full)
	partial := newAssetTestRepo(t, base, remote, "partial", true)
	syncAssetTestRepo(t, partial)
	writeAssetTestFile(t, partial, path, "same", 100)
	if _, err := partial.EnsureAsset(path, nil); err != nil {
		t.Fatal(err)
	}
	expected := assetTestFile(t, full, path)
	info, err := os.Stat(partial.absPath(path))
	if err != nil || info.ModTime().UnixMilli() != expected.Updated {
		t.Fatalf("downloaded timestamp not aligned: %v %v", info, err)
	}
	if _, err = partial.Index("verify", true, nil); err != nil {
		t.Fatal(err)
	}
	if actual := assetTestFile(t, partial, path); !sameAssetVersion(actual, expected) {
		t.Fatalf("downloaded version changed on scan: %+v", actual)
	}
}

func TestAssetTimestampPreservesLocalEdit(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		name := "checkout"
		if recovery {
			name = "recovery"
		}
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", true)
			path := "/public/icon.png"
			writeAssetTestFile(t, repo, path, "same", 200)
			syncAssetTestRepo(t, repo)
			target, err := repo.Latest()
			if err != nil {
				t.Fatal(err)
			}
			file := assetTestFile(t, repo, path)
			writeAssetTestFile(t, repo, path, "edit", 300)
			if recovery {
				repo.assetDownloads.state.Pending = &assetApply{Index: target, Base: target,
					Deferred: map[string]*entity.File{}, Before: map[string]*entity.File{path: file},
					Upserts: []*entity.File{file}}
				if err = repo.saveAssetState(); err != nil {
					t.Fatal(err)
				}
				_, _, err = repo.RecoverAssetDownloads(nil)
			} else {
				err = repo.checkoutFile(file, repo.DataPath, 1, 1, nil, file)
			}
			if !errors.Is(err, ErrIndexFileChanged) {
				t.Fatalf("local edit not rejected: %v", err)
			}
			data, err := os.ReadFile(repo.absPath(path))
			if err != nil || string(data) != "edit" {
				t.Fatalf("local content changed: %q %v", data, err)
			}
			info, err := os.Stat(repo.absPath(path))
			if err != nil || info.ModTime().Unix() != 1700000300 {
				t.Fatalf("local timestamp changed: %v %v", info, err)
			}
		})
	}
}
