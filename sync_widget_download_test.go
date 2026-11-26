package dejavu

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

func uploadWidgetDownloadFixture(t *testing.T, repo *Repo, entries map[string]string) *entity.Index {
	t.Helper()
	latest, err := repo.Latest()
	if err != nil {
		t.Fatal(err)
	}
	index := *latest
	index.ID = util.RandHash()
	index.Files = append([]string(nil), latest.Files...)
	for name, content := range entries {
		file := entity.NewFile(name, int64(len(content)), time.Unix(1700000002, 0).UnixMilli())
		chunkID := util.Hash([]byte(content))
		file.Chunks = []string{chunkID}
		if err = repo.store.PutChunk(&entity.Chunk{ID: chunkID, Data: []byte(content)}); err != nil {
			t.Fatal(err)
		}
		if err = repo.store.PutFile(file); err != nil {
			t.Fatal(err)
		}
		index.Files = append(index.Files, file.ID)
		index.Size += file.Size
	}
	index.Count = len(index.Files)
	traffic := &TrafficStat{m: &sync.Mutex{}}
	if err = repo.uploadCloud(nil, &index, &entity.Index{}, nil, traffic); err != nil {
		t.Fatal(err)
	}
	if err = repo.store.PutIndex(&index); err != nil {
		t.Fatal(err)
	}
	if err = repo.UpdateLatest(&index); err != nil {
		t.Fatal(err)
	}
	if err = repo.updateCloudIndexes(&index, traffic, nil, buildSyncLinkView(nil, nil)); err != nil {
		t.Fatal(err)
	}
	return &index
}

func TestWidgetSyncDownloadRepair(t *testing.T) {
	for _, mode := range []string{"no asset policy", "full assets", "on-demand assets"} {
		t.Run(mode, func(t *testing.T) {
			base := t.TempDir()
			remote := filepath.Join(base, "cloud")
			source := newAssetTestRepo(t, base, remote, "source", false)
			target := newAssetTestRepo(t, base, remote, "target", mode == "on-demand assets")
			if mode == "no asset policy" {
				target.assetDownloads = nil
			}
			target.repairWidgetPathsOnDownload = true
			const only = "/widgets/example/a/script.js"
			const collision = "/widgets/example/b/script.js"
			snapshot := uploadWidgetDownloadFixture(t, source, map[string]string{
				"/widgets/example/a\\script.js": "cloud",
				"/widgets/example/b\\script.js": "legacy copy",
				collision:                       "canonical",
			})
			// 相同大小和时间戳的本地旧文件也要被覆盖并留档。
			writeAssetTestFile(t, target, only, "local", 2)
			if _, err := target.Index("target", false, nil); err != nil {
				t.Fatal(err)
			}
			merge, _, err := target.SyncDownload(nil)
			if err != nil {
				t.Fatal(err)
			}
			for p, want := range map[string]string{only: "cloud", collision: "canonical"} {
				contents, err := os.ReadFile(target.absPath(p))
				if err != nil || string(contents) != want {
					t.Fatalf("%s = %q, want %q: %v", p, contents, want, err)
				}
				assetTestFile(t, target, p)
			}
			if len(merge.HistoryPaths) != 2 {
				t.Fatalf("unexpected history paths: %v", merge.HistoryPaths)
			}
			history := filepath.Join(target.HistoryPath, merge.Time.Format("2006-01-02-150405")+"-sync")
			for _, p := range merge.HistoryPaths {
				want := "local"
				if strings.HasPrefix(p, widgetPathConflictPrefix) {
					want = "legacy copy"
				}
				contents, err := os.ReadFile(filepath.Join(history, p))
				if err != nil || string(contents) != want {
					t.Fatalf("history %s = %q, want %q: %v", p, contents, want, err)
				}
			}
			if _, err = target.Index("target again", false, nil); err != nil {
				t.Fatal(err)
			}
			repeated, _, err := target.SyncDownload(nil)
			if err != nil || repeated.DataChanged() || repeated.HasHistory() {
				t.Fatalf("downloading the same cloud snapshot changed data or history: %+v, %v", repeated, err)
			}
			_, unchangedCloud, err := target.downloadCloudLatest(nil)
			if err != nil || unchangedCloud.ID != snapshot.ID {
				t.Fatal("download changed the cloud snapshot", err)
			}
		})
	}
}

func TestWidgetSyncDownloadRejectsAmbiguousPaths(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	source := newAssetTestRepo(t, base, remote, "source", false)
	target := newAssetTestRepo(t, base, remote, "target", false)
	target.repairWidgetPathsOnDownload = true
	uploadWidgetDownloadFixture(t, source, map[string]string{
		"/widgets/example/a\\b/c.js": "first",
		"/widgets/example/a/b\\c.js": "second",
		"/incoming.txt":              "same batch",
	})
	if _, _, err := target.SyncDownload(nil); err == nil || !strings.Contains(err.Error(), "multiple legacy entries") {
		t.Fatalf("expected unresolved widget conflict, got %v", err)
	}
	if _, err := os.Stat(target.absPath("/incoming.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("download applied part of the rejected batch: %v", err)
	}
}

func TestWidgetSyncDownloadRejectsEquivalentPathSpellings(t *testing.T) {
	repaired := &entity.File{Path: "/widgets/example/icons/caf\u00e9.js"}
	other := &entity.File{Path: "/widgets/example/icons/CAFE\u0301.js"}
	plan := widgetDownloadPlan{
		files:   []*entity.File{repaired, other},
		targets: map[string]*entity.File{repaired.Path: repaired},
	}
	if err := plan.checkTargetConflicts(); err == nil {
		t.Fatal("equivalent cloud spelling was accepted")
	}
	if _, err := plan.applyDownloadDiff(&MergeResult{}, []*entity.File{other}, syncLinkView{}); err == nil {
		t.Fatal("equivalent local spelling was accepted")
	}
}
