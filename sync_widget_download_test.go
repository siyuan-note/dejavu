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
			repeated, _, err = target.Sync(nil)
			if err != nil || repeated.DataChanged() || repeated.HasHistory() {
				t.Fatalf("switching to two-way sync changed data or history: %+v, %v", repeated, err)
			}
			_, unchangedCloud, err := target.downloadCloudLatest(nil)
			if err != nil || unchangedCloud.ID != snapshot.ID {
				t.Fatal("download changed the cloud snapshot", err)
			}
		})
	}
}

func TestWidgetSyncRepairsFirstDownload(t *testing.T) {
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
			seed, err := target.Latest()
			if err != nil {
				t.Fatal(err)
			}
			if err = target.UpdateLatestSync(seed); err != nil {
				t.Fatal(err)
			}
			const repaired = "/widgets/example/a/script.js"
			const canonical = "/widgets/example/b/script.js"
			snapshot := uploadWidgetDownloadFixture(t, source, map[string]string{
				"/widgets/example/a\\script.js": "cloud",
				"/widgets/example/b\\script.js": "legacy copy",
				canonical:                       "canonical copy",
				"/assets/example.txt":           "deferred in on-demand mode",
			})
			merge, _, err := target.Sync(nil)
			if err != nil {
				t.Fatal(err)
			}
			for name, want := range map[string]string{repaired: "cloud", canonical: "canonical copy"} {
				data, readErr := os.ReadFile(target.absPath(name))
				if readErr != nil || string(data) != want {
					t.Fatalf("read %s: got %q, want %q (%v)", name, data, want, readErr)
				}
				assetTestFile(t, target, name)
			}
			if len(merge.HistoryPaths) != 1 || !strings.HasPrefix(merge.HistoryPaths[0], widgetPathConflictPrefix) || len(merge.ConflictDetails) != 0 {
				t.Fatalf("unexpected first merge: %+v", merge)
			}
			history := filepath.Join(target.HistoryPath, merge.Time.Format("2006-01-02-150405")+"-sync", merge.HistoryPaths[0])
			data, err := os.ReadFile(history)
			if err != nil || string(data) != "legacy copy" {
				t.Fatalf("legacy history = %q: %v", data, err)
			}
			if mode == "on-demand assets" {
				asset := assetTestFile(t, target, "/assets/example.txt")
				if !target.deferredVersion(asset) {
					t.Fatal("widget path repair materialized an unrelated deferred asset")
				}
			}
			if _, err = target.Index("verify repaired index", false, nil); err != nil {
				t.Fatal(err)
			}
			repeated, _, err := target.Sync(nil)
			if err != nil || repeated.DataChanged() || repeated.HasHistory() {
				t.Fatalf("repeating two-way sync changed data or history: %+v, %v", repeated, err)
			}
			_, unchanged, err := target.downloadCloudLatest(nil)
			if err != nil || unchanged.ID != snapshot.ID {
				t.Fatal("repair without local changes published a different cloud snapshot", err)
			}
		})
	}
}

func TestWidgetSyncUsesThreeWayMergeAndPublishesRepairedObjects(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	source := newAssetTestRepo(t, base, remote, "source", false)
	target := newAssetTestRepo(t, base, remote, "target", false)
	target.assetDownloads = nil
	target.repairWidgetPathsOnDownload = true
	const changed = "/widgets/example/a/script.js"
	const removed = "/widgets/example/b/script.js"
	const received = "/widgets/example/c/script.js"
	snapshot := uploadWidgetDownloadFixture(t, source, map[string]string{
		changed:                         "base",
		"/widgets/example/b\\script.js": "unchanged until cloud removes it",
	})
	if _, _, err := target.SyncDownload(nil); err != nil {
		t.Fatal(err)
	}
	// 使用包含旧拼写的同步点模拟升级前记录或中断恢复。
	if err := target.store.PutIndex(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := target.UpdateLatestSync(snapshot); err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, target, changed, "locally edited", 3)
	if _, err := target.Index("local edit", false, nil); err != nil {
		t.Fatal(err)
	}
	remoteEditor := newAssetTestRepo(t, base, remote, "remote-editor", false)
	uploadWidgetDownloadFixture(t, remoteEditor, map[string]string{
		"/widgets/example/a\\script.js": "remotely edited",
		"/widgets/example/c\\script.js": "cloud only",
	})
	merge, _, err := target.Sync(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(merge.ConflictDetails) != 1 || merge.ConflictDetails[0].Path != changed || merge.ConflictDetails[0].Winner != ConflictSideLocal {
		t.Fatalf("unexpected merge conflicts: %+v", merge.ConflictDetails)
	}
	if len(merge.HistoryPaths) != 1 || merge.HistoryPaths[0] != changed {
		t.Fatalf("unexpected merge history: %v", merge.HistoryPaths)
	}
	history := filepath.Join(target.HistoryPath, merge.Time.Format("2006-01-02-150405")+"-sync", changed)
	data, err := os.ReadFile(history)
	if err != nil || string(data) != "remotely edited" {
		t.Fatalf("remote conflict history = %q: %v", data, err)
	}
	if _, err = os.Lstat(target.absPath(removed)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("one-sided cloud removal did not use the repaired baseline: %v", err)
	}
	peer := newAssetTestRepo(t, base, remote, "peer", false)
	if _, _, err = peer.SyncDownload(nil); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{changed: "locally edited", received: "cloud only"} {
		data, err = os.ReadFile(peer.absPath(name))
		if err != nil || string(data) != want {
			t.Fatalf("published %s = %q, want %q: %v", name, data, want, err)
		}
	}
	if files, err := peer.GetFiles(peer.latestSync()); err != nil {
		t.Fatal(err)
	} else {
		for _, file := range files {
			if strings.Contains(file.Path, "\\") || file.Path == removed {
				t.Fatalf("unexpected path in published snapshot: %q", file.Path)
			}
		}
	}
}

func TestWidgetSyncPublishesLocalChangesWhenRecheckMakesCheckoutUnnecessary(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	source := newAssetTestRepo(t, base, remote, "source", false)
	target := newAssetTestRepo(t, base, remote, "target", false)
	target.assetDownloads = nil
	target.repairWidgetPathsOnDownload = true
	const name = "/widgets/example/a/script.js"
	const edited = "/edited.txt"
	uploadWidgetDownloadFixture(t, source, map[string]string{"/widgets/example/a\\script.js": "from cloud"})
	target.SetCheckoutHooks(CheckoutHooks{Apply: func() error {
		writeAssetTestFile(t, target, name, "from cloud", 2)
		writeAssetTestFile(t, target, edited, "saved during sync", 3)
		return nil
	}})
	merge, _, err := target.Sync(nil)
	if err != nil || merge.DataChanged() {
		t.Fatalf("unexpected rechecked merge: %+v, %v", merge, err)
	}
	peer := newAssetTestRepo(t, base, remote, "peer", false)
	if _, _, err = peer.SyncDownload(nil); err != nil {
		t.Fatal(err)
	}
	for p, want := range map[string]string{name: "from cloud", edited: "saved during sync"} {
		contents, readErr := os.ReadFile(peer.absPath(p))
		if readErr != nil || string(contents) != want {
			t.Fatalf("published %s = %q, want %q: %v", p, contents, want, readErr)
		}
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
	if _, _, err := target.Sync(nil); err == nil || !strings.Contains(err.Error(), "multiple legacy entries") {
		t.Fatalf("expected two-way sync to reject the unresolved widget conflict, got %v", err)
	}
	if _, err := os.Stat(target.absPath("/incoming.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("two-way sync applied part of the rejected batch: %v", err)
	}
}

func TestWidgetSyncDownloadRejectsEquivalentPathSpellings(t *testing.T) {
	repaired := &entity.File{Path: "/widgets/example/icons/caf\u00e9.js"}
	other := &entity.File{Path: "/widgets/example/icons/CAFE\u0301.js"}
	plan := downloadPathPlan{
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
