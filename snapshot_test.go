package dejavu

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSnapshotCreationAndMemo(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "local", false)
	latest, err := repo.Latest()
	if err != nil {
		t.Fatal(err)
	}
	index, created, err := repo.IndexWithResult("unused", true, nil)
	if err != nil || created || index.ID != latest.ID || index.Memo != "seed" {
		t.Fatalf("unchanged: index=%v created=%v err=%v", index, created, err)
	}
	writeAssetTestFile(t, repo, "/new.txt", "new content", 2)
	index, created, err = repo.IndexWithResult("manual", true, nil)
	if err != nil || !created || index.ID == latest.ID || index.Memo != "manual" {
		t.Fatalf("created: index=%v created=%v err=%v", index, created, err)
	}
	before := *index
	if err = repo.SetSnapshotMemo(index.ID, "edited\n备注"); err != nil {
		t.Fatal(err)
	}
	indexCache.Wait()
	indexCache.Clear()
	updated, err := repo.Latest()
	if err != nil {
		t.Fatal(err)
	}
	if updated.Memo != "edited\n备注" || index.Memo != before.Memo {
		t.Fatalf("memo update mutated original or failed: %v", updated)
	}
	updated.Memo = before.Memo
	if !reflect.DeepEqual(*updated, before) {
		t.Fatalf("memo update changed snapshot data: %v", updated)
	}
	if err = repo.SetSnapshotMemo("../invalid", "memo"); err == nil {
		t.Fatal("invalid ID accepted")
	}
}

func TestSnapshotCheckChanges(t *testing.T) {
	for _, action := range []string{"none", "add", "update", "remove", "ignored", "cache"} {
		t.Run(action, func(t *testing.T) {
			base := t.TempDir()
			repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "local", false)
			writeAssetTestFile(t, repo, "/other.txt", "other", 2)
			if _, err := repo.Index("base", true, nil); err != nil {
				t.Fatal(err)
			}
			switch action {
			case "add":
				writeAssetTestFile(t, repo, "/new.txt", "new", 3)
			case "update":
				writeAssetTestFile(t, repo, "/other.txt", "changed", 3)
			case "remove":
				if err := os.Remove(repo.absPath("/other.txt")); err != nil {
					t.Fatal(err)
				}
			case "ignored":
				writeAssetTestFile(t, repo, "/.ignored", "ignored", 3)
			case "cache":
				if err := os.WriteFile(filepath.Join(repo.Path, "full-latest.json"), []byte("invalid"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			before := snapshotTestFiles(t, repo.Path)
			changed, err := repo.CheckSnapshot()
			want := action == "add" || action == "update" || action == "remove"
			if err != nil || changed != want {
				t.Fatalf("changed=%v want=%v err=%v", changed, want, err)
			}
			if !reflect.DeepEqual(before, snapshotTestFiles(t, repo.Path)) {
				t.Fatal("check changed repository files")
			}
		})
	}
}

func snapshotTestFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	ret := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		ret[path] = string(data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return ret
}

func TestSnapshotCheckDeferredAssets(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	full := newAssetTestRepo(t, base, remote, "full", false)
	writeAssetTestFile(t, full, "/assets/test.bin", "asset", 2)
	syncAssetTestRepo(t, full)
	partial := newAssetTestRepo(t, base, remote, "partial", true)
	syncAssetTestRepo(t, partial)
	before := snapshotTestFiles(t, filepath.Join(base, "partial"))
	changed, err := partial.CheckSnapshot()
	if err != nil || changed {
		t.Fatalf("deferred asset treated as deletion: changed=%v err=%v", changed, err)
	}
	if !reflect.DeepEqual(before, snapshotTestFiles(t, filepath.Join(base, "partial"))) {
		t.Fatal("check materialized assets or changed state")
	}
	partial.IgnoreLines = append(partial.IgnoreLines, "/assets/test.bin")
	changed, err = partial.CheckSnapshot()
	if err != nil || !changed {
		t.Fatalf("ignored deferred asset: changed=%v err=%v", changed, err)
	}
	if !reflect.DeepEqual(before, snapshotTestFiles(t, filepath.Join(base, "partial"))) {
		t.Fatal("check materialized ignored asset")
	}
}

func TestSnapshotCheckInvalidLatest(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "local", false)
	path := filepath.Join(repo.Path, "refs", "latest")
	if err := os.WriteFile(path, []byte("invalid"), 0644); err != nil {
		t.Fatal(err)
	}
	before := snapshotTestFiles(t, repo.Path)
	if _, err := repo.CheckSnapshot(); err == nil {
		t.Fatal("invalid latest accepted")
	}
	if !reflect.DeepEqual(before, snapshotTestFiles(t, repo.Path)) {
		t.Fatal("invalid latest changed")
	}
}

func TestSnapshotInvalidKeyPreservesIndex(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "local", false)
	latest, err := repo.Latest()
	if err != nil {
		t.Fatal(err)
	}
	before := snapshotTestFiles(t, repo.Path)
	repo.store.AesKey = []byte("abcdef0123456789abcdef0123456789")
	if err = repo.SetSnapshotMemo(latest.ID, "changed"); err == nil {
		t.Fatal("wrong key accepted for memo edit")
	}
	if _, err = repo.CheckSnapshot(); err == nil {
		t.Fatal("wrong key accepted for check")
	}
	if !reflect.DeepEqual(before, snapshotTestFiles(t, repo.Path)) {
		t.Fatal("failed operation changed original data")
	}
}
