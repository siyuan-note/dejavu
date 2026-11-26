package dejavu

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
)

func TestSyncPrefersCanonicalDownloadPathsAcrossRepository(t *testing.T) {
	for _, mode := range []string{"two-way", "download-only"} {
		t.Run(mode, func(t *testing.T) {
			base := t.TempDir()
			remote := filepath.Join(base, "cloud")
			source := newAssetTestRepo(t, base, remote, "source", false)
			target := newAssetTestRepo(t, base, remote, "target", true)
			target.preferCanonicalPathsOnDownload = true
			seed, err := target.Latest()
			if err != nil {
				t.Fatal(err)
			}
			if err = target.UpdateLatestSync(seed); err != nil {
				t.Fatal(err)
			}
			const asset = "/assets/example/photo.png"
			const custom = "/custom/settings.json"
			snapshot := uploadWidgetDownloadFixture(t, source, map[string]string{
				"/assets/example\\photo.png": "legacy attachment",
				"/custom\\settings.json":     "legacy settings",
				asset:                        "canonical attachment",
				custom:                       "canonical settings",
			})
			sync := target.Sync
			if mode == "download-only" {
				sync = target.SyncDownload
			}
			merge, _, err := sync(nil)
			if err != nil {
				t.Fatal(err)
			}
			if !target.deferredVersion(assetTestFile(t, target, asset)) {
				t.Fatal("downloaded the canonical attachment eagerly")
			}
			if fetched, err := target.EnsureAsset(asset, nil); err != nil || !fetched {
				t.Fatalf("download canonical attachment: %v, %v", fetched, err)
			}
			if len(merge.HistoryPaths) != 2 || len(merge.ConflictDetails) != 0 {
				t.Fatalf("unexpected merge: %+v", merge)
			}
			history := filepath.Join(target.HistoryPath, merge.Time.Format("2006-01-02-150405")+"-sync")
			wantHistory := map[string]string{asset: "legacy attachment", custom: "legacy settings"}
			for _, p := range merge.HistoryPaths {
				rest, ok := strings.CutPrefix(p, downloadPathConflictPrefix)
				_, file, hasFile := strings.Cut(rest, "/")
				if !ok || !hasFile || wantHistory["/"+file] == "" {
					t.Fatalf("unexpected archive path %q", p)
				}
				contents, err := os.ReadFile(filepath.Join(history, p))
				if err != nil || string(contents) != wantHistory["/"+file] {
					t.Fatalf("read archive %q: %q (%v)", p, contents, err)
				}
				delete(wantHistory, "/"+file)
			}
			for p, want := range map[string]string{asset: "canonical attachment", custom: "canonical settings"} {
				contents, err := os.ReadFile(target.absPath(p))
				if err != nil || string(contents) != want {
					t.Fatalf("read %q: %q, want %q (%v)", p, contents, want, err)
				}
			}
			if _, err = target.Index("verify canonical files", false, nil); err != nil {
				t.Fatal(err)
			}
			if merge, _, err = sync(nil); err != nil || merge.DataChanged() || merge.HasHistory() {
				t.Fatalf("repeated sync rewrote data or history: %+v, %v", merge, err)
			}
			if _, cloud, err := target.downloadCloudLatest(nil); err != nil || cloud.ID != snapshot.ID {
				t.Fatalf("download path repair changed the cloud reference: %v", err)
			}
		})
	}
}

func TestCanonicalDownloadPathPreferenceRequiresExistingCanonicalFile(t *testing.T) {
	legacy := &entity.File{Path: "/assets/example\\photo.png"}
	canonical := &entity.File{Path: "/assets/example/photo.png"}
	files := []*entity.File{legacy, canonical}
	plan, err := (&Repo{repairWidgetPathsOnDownload: true}).planDownloadPaths(files)
	if err != nil || len(plan.files) != 2 || len(plan.discarded) != 0 {
		t.Fatalf("widget-only policy changed an attachment: %+v, %v", plan, err)
	}
	plan, err = (&Repo{preferCanonicalPathsOnDownload: true}).planDownloadPaths([]*entity.File{legacy})
	if err != nil || len(plan.files) != 1 || plan.files[0] != legacy || len(plan.discarded) != 0 {
		t.Fatalf("missing canonical attachment was synthesized: %+v, %v", plan, err)
	}
}
