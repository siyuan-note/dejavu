package dejavu

import (
	"strings"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

func TestAppearancePartialCloudRestoreRequiresCompleteSnapshot(t *testing.T) {
	for _, p := range []string{"/themes/example/theme.css", appearanceFormatPath,
		"/storage/appearance-v1/themes/example/" + strings.Repeat("0", 64) + ".sypkg",
		"/storage/appearance-v1/unknown.json"} {
		t.Run(p, func(t *testing.T) {
			repo := newAppearanceSyncTestRepo(t, t.TempDir(), t.TempDir(), "partial", false)
			repo.cloud = nil
			writeAssetTestFile(t, repo, p, "original", 10)
			file := entity.NewFile(p, 1, 1700000020000)
			file.Chunks = []string{util.Hash([]byte("x"))}
			if _, err := repo.CheckoutFilesFromCloud([]*entity.File{file}, nil); err == nil || !strings.Contains(err.Error(), "complete snapshot") {
				t.Fatalf("partial appearance restore was not rejected before network access: %v", err)
			}
			if got := readAppearanceTestFile(t, repo, p); got != "original" {
				t.Fatalf("partial restore changed source: %s", got)
			}
		})
	}
}

func TestAppearancePartialCloudRestorePreservesOrdinaryBehavior(t *testing.T) {
	for _, test := range []struct {
		path    string
		enabled bool
	}{{"/note.txt", true}, {appearanceFormatPath, false},
		{"/storage/appearance-v1/themes/example/" + strings.Repeat("0", 64) + ".sypkg", false}} {
		t.Run(test.path, func(t *testing.T) {
			repo := newAssetTestRepo(t, t.TempDir(), t.TempDir(), "ordinary", false)
			repo.appearanceSyncEnabled = test.enabled
			data := []byte("ordinary file restore")
			file := entity.NewFile(test.path, int64(len(data)), 1700000020000)
			file.Chunks = []string{util.Hash(data)}
			if err := repo.store.PutChunk(&entity.Chunk{ID: file.Chunks[0], Data: data}); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.CheckoutFilesFromCloud([]*entity.File{file}, nil); err != nil {
				t.Fatal(err)
			}
			if got := readAppearanceTestFile(t, repo, file.Path); got != string(data) {
				t.Fatalf("ordinary partial restore changed: %s", got)
			}
		})
	}
}
