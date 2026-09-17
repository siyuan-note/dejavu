package dejavu

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAppearanceWholeIgnoreSkipsUnknownCloudAndLocalFiles(t *testing.T) {
	for _, rule := range []string{"/themes/example/", "/storage/"} {
		for _, manual := range []bool{false, true} {
			t.Run(rule+map[bool]string{false: "sync", true: "download"}[manual], func(t *testing.T) {
				base, remote := t.TempDir(), t.TempDir()
				old := newAssetTestRepo(t, base, remote, "old", false)
				current := newAppearanceSyncTestRepo(t, base, remote, "current", false)
				unknown := "/storage/appearance-v1/themes/example/unknown.json"
				badArchive := "/storage/appearance-v1/themes/example/" + strings.Repeat("0", 64) + ".sypkg"
				writeAssetTestFile(t, old, unknown, "unknown ignored file", 10)
				writeAssetTestFile(t, old, badArchive, "invalid ignored archive", 10)
				writeAssetTestFile(t, old, "/note.txt", "ordinary note", 10)
				writeAssetTestFile(t, old, old.ignoreRulePath, rule, 10)
				syncAppearanceTestRepo(t, old)
				for _, p := range []string{unknown, badArchive} {
					for _, id := range assetTestFile(t, old, p).Chunks {
						if err := os.Remove(filepath.Join(remote, "main", "objects", id[:2], id[2:])); err != nil {
							t.Fatal(err)
						}
					}
				}
				var err error
				if manual {
					_, _, err = current.SyncDownload(nil)
				} else {
					_, _, err = current.Sync(nil)
				}
				if err != nil {
					t.Fatalf("ignored unavailable contents blocked ordinary notes: %v", err)
				}
				if got := readAppearanceTestFile(t, current, "/note.txt"); got != "ordinary note" {
					t.Fatalf("ordinary note did not sync: %s", got)
				}
				for _, p := range []string{unknown, badArchive} {
					if _, err = os.Stat(current.absPath(p)); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("ignored unknown file applied: %s %v", p, err)
					}
				}
				current.appearanceIgnoreLines, current.IgnoreLines = []string{rule}, []string{rule}
				writeAssetTestFile(t, current, unknown, "private ignored content", 20)
				index, err := current.Index("ignored local package", true, nil)
				if err != nil {
					t.Fatal(err)
				}
				files, err := current.GetFiles(index)
				if err != nil {
					t.Fatal(err)
				}
				for _, file := range files {
					if file.Path == unknown || file.Path == badArchive {
						t.Fatalf("ignored local contents entered snapshot: %s", file.Path)
					}
				}
				if changed, err := current.CheckSnapshot(); err != nil || changed {
					t.Fatalf("ignored contents keep changing snapshot: %v %v", changed, err)
				}
			})
		}
	}
}

func TestAppearanceWholeIgnoreDoesNotPrefetchPeerPayload(t *testing.T) {
	repo, remote := newChunkSourceTestRepo(t)
	repo.appearanceSyncEnabled = true
	repo.appearanceIgnoreLines = []string{"/themes/example/"}
	file, chunks := appearanceSourceFile("/themes/example", "ignored peer payload")
	objects := encodeAppearanceSourceFile(t, repo, file, chunks)
	if _, err := remote.UploadBytes("objects/"+file.ID[:2]+"/"+file.ID[2:], objects[file.ID], false); err != nil {
		t.Fatal(err)
	}
	source := &testChunkSource{chunks: objects}
	repo.SetChunkSource(source)
	stat, files, err := repo.downloadCloudFilesPut([]string{file.ID}, nil)
	if err != nil || len(files) != 1 || stat.PeerCount != 0 || stat.PeerFallbackCount != 1 || stat.PrefetchedChunkCount != 0 {
		t.Fatalf("ignored peer file did not use cloud metadata only: %+v %+v %v", files, stat, err)
	}
	for _, id := range file.Chunks {
		if _, downloaded := source.downloads.Load(id); downloaded {
			t.Fatal("ignored package prefetched peer payload")
		}
	}
}
