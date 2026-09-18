package dejavu

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/restic/chunker"
	"github.com/siyuan-note/dejavu/entity"
)

func TestIndexReusedFileTimestamp(t *testing.T) {
	for _, size := range []int{13, chunker.MinSize + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			testIndexReusedFileTimestamp(t, size)
		})
	}
}

func testIndexReusedFileTimestamp(t *testing.T, size int) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	p := "/plugins/example/plugin.json"
	oldContent, newContent := strings.Repeat("a", size), strings.Repeat("b", size)
	writeAssetTestFile(t, repo, p, oldContent, 10)
	first, err := repo.Index("first", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, repo, p, "cloud package", 20)
	if _, err = repo.Index("cloud", true, nil); err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, repo, p, newContent, 10)
	latest, err := repo.Index("reinstalled", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	file := assetTestFile(t, repo, p)
	data, err := repo.openFile(file)
	if err != nil || string(data) != newContent {
		t.Fatalf("snapshot reused stale content: %v", err)
	}
	info, err := os.Stat(repo.absPath(p))
	if err != nil || info.ModTime().UnixMilli() != file.Updated || file.ID != entity.NewFile(p, file.Size, file.Updated).ID {
		t.Fatalf("snapshot identity does not match disk: %+v %v", file, err)
	}
	stable, err := repo.Index("unchanged", true, nil)
	if err != nil || stable.ID != latest.ID {
		t.Fatalf("index did not converge: %v", err)
	}
	files, err := repo.GetFiles(first)
	if err != nil {
		t.Fatal(err)
	}
	for _, previous := range files {
		if previous.Path == p {
			data, err = repo.openFile(previous)
			if err != nil || string(data) != oldContent {
				t.Fatalf("previous snapshot changed: %v", err)
			}
		}
	}
	for _, snapshot := range []struct {
		index   *entity.Index
		content string
	}{{first, oldContent}, {latest, newContent}} {
		if _, _, err = repo.Checkout(snapshot.index.ID, nil); err != nil {
			t.Fatal(err)
		}
		data, err = os.ReadFile(repo.absPath(p))
		if err != nil || string(data) != snapshot.content {
			t.Fatalf("snapshot checkout restored wrong content: %v", err)
		}
	}
}

func TestIndexReusedTimestampSameContent(t *testing.T) {
	repo := newAssetTestRepo(t, t.TempDir(), t.TempDir(), "device", false)
	p := "/plugins/unchanged/plugin.json"
	writeAssetTestFile(t, repo, p, "same package", 10)
	if _, err := repo.Index("first", true, nil); err != nil {
		t.Fatal(err)
	}
	previous := assetTestFile(t, repo, p)
	writeAssetTestFile(t, repo, p, "cloud package", 20)
	if _, err := repo.Index("cloud", true, nil); err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, repo, p, "same package", 10)
	if _, err := repo.Index("reinstalled", true, nil); err != nil {
		t.Fatal(err)
	}
	file := assetTestFile(t, repo, p)
	info, err := os.Stat(repo.absPath(p))
	if err != nil || file.ID != previous.ID || info.ModTime().UnixMilli() != previous.Updated {
		t.Fatalf("unchanged content timestamp renewed: %+v %v", file, err)
	}
}

func TestIndexReusedTimestampPreservesConcurrentEdit(t *testing.T) {
	repo := newAssetTestRepo(t, t.TempDir(), t.TempDir(), "device", false)
	p := "/plugins/concurrent/plugin.json"
	writeAssetTestFile(t, repo, p, "old", 10)
	if _, err := repo.Index("first", true, nil); err != nil {
		t.Fatal(err)
	}
	previous := assetTestFile(t, repo, p)
	candidate := *previous
	candidate.Chunks = []string{strings.Repeat("0", 40)}
	writeAssetTestFile(t, repo, p, "edited", 20)
	if err := repo.putIndexedFile(&candidate); !errors.Is(err, ErrIndexFileChanged) {
		t.Fatalf("concurrent edit not rejected: %v", err)
	}
	info, err := os.Stat(repo.absPath(p))
	if err != nil || info.ModTime().Unix() != 1700000020 {
		t.Fatalf("concurrent edit timestamp overwritten: %v", err)
	}
	data, err := os.ReadFile(repo.absPath(p))
	if err != nil || string(data) != "edited" {
		t.Fatalf("concurrent edit overwritten: %q %v", data, err)
	}
}

func TestAppearanceSyncReusedPluginTimestamp(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	repo := newAppearanceSyncTestRepo(t, base, remote, "first", false)
	p := "/plugins/example/plugin.json"
	writeAppearanceTestPackage(t, repo, "/themes/example", map[string]string{"theme.json": "{}", "theme.css": "body {}"}, 1, false)
	const zipTime = int64(315532800 - 1700000000)
	writeAssetTestFile(t, repo, p, "first package", zipTime)
	syncAssetTestRepo(t, repo)
	writeAssetTestFile(t, repo, p, "cloud package", 20)
	syncAssetTestRepo(t, repo)
	writeAssetTestFile(t, repo, p, "later package", zipTime)
	syncAssetTestRepo(t, repo)
	other := newAppearanceSyncTestRepo(t, base, remote, "second", false)
	syncAssetTestRepo(t, other)
	data, err := os.ReadFile(other.absPath(p))
	if err != nil || string(data) != "later package" {
		t.Fatalf("reinstalled plugin not synced: %q %v", data, err)
	}
	syncAssetTestRepo(t, repo)
	syncAssetTestRepo(t, other)
}

func TestIndexReusedTimestampPreservesCorruptObject(t *testing.T) {
	repo := newAssetTestRepo(t, t.TempDir(), t.TempDir(), "device", false)
	p := "/plugins/corrupt/plugin.json"
	writeAssetTestFile(t, repo, p, "package", 10)
	file := entity.NewFile(p, 7, time.Unix(1700000010, 0).UnixMilli())
	_, object := repo.store.AbsPath(file.ID)
	if err := os.MkdirAll(filepath.Dir(object), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(object, []byte("corrupt"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Index("corrupt", true, nil); err == nil || errors.Is(err, ErrIndexFileChanged) {
		t.Fatalf("corrupt object not rejected: %v", err)
	}
	data, err := os.ReadFile(object)
	if err != nil || string(data) != "corrupt" {
		t.Fatalf("corrupt object overwritten: %q %v", data, err)
	}
	info, err := os.Stat(repo.absPath(p))
	if err != nil || info.ModTime().UnixMilli() != file.Updated {
		t.Fatalf("source timestamp changed after authentication failure: %v", err)
	}
}
