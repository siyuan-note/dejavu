package dejavu

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
)

func TestAssetDownloadsRecoveryAcknowledgement(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	full := newAssetTestRepo(t, base, remote, "full", false)
	writeAssetTestFile(t, full, "/document.txt", "before", 100)
	writeAssetTestFile(t, full, "/assets/file.bin", "resource", 100)
	syncAssetTestRepo(t, full)
	partial := newAssetTestRepo(t, base, remote, "partial", true)
	syncAssetTestRepo(t, partial)
	before := assetTestFile(t, partial, "/document.txt")
	writeAssetTestFile(t, full, "/document.txt", "after", 101)
	writeAssetTestFile(t, full, "/second.txt", "second", 101)
	syncAssetTestRepo(t, full)
	target, err := full.Latest()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = partial.DownloadIndex(target.ID, nil); err != nil {
		t.Fatal(err)
	}
	after, second := assetTestFile(t, full, "/document.txt"), assetTestFile(t, full, "/second.txt")
	asset := assetTestFile(t, full, "/assets/file.bin")
	partial.assetDownloads.state.Pending = &assetApply{Index: target, Base: target,
		Deferred: map[string]*entity.File{asset.Path: asset}, Upserts: []*entity.File{after, second},
		Before: map[string]*entity.File{before.Path: before, second.Path: nil}}
	if err = partial.saveAssetState(); err != nil {
		t.Fatal(err)
	}
	_, missingChunk := partial.store.AbsPath(second.Chunks[0])
	if err = os.Remove(missingChunk); err != nil {
		t.Fatal(err)
	}
	if err = partial.ConfigureAssetDownloads(true, partial.assetDownloads.path, "test-scope"); err != nil {
		t.Fatal(err)
	}
	if data, readErr := os.ReadFile(partial.absPath(before.Path)); readErr != nil || string(data) != "before" {
		t.Fatalf("configuration replayed document writes: %q %v", data, readErr)
	}
	if _, err = partial.EnsureAsset(asset.Path, nil); !errors.Is(err, ErrAssetApplyPending) {
		t.Fatalf("resource read replayed pending document writes: %v", err)
	}
	id, changes, err := partial.RecoverAssetDownloads(nil)
	if err == nil || id == "" || changes == nil || len(changes.Upserts) != 1 || len(changes.Removes) != 1 {
		t.Fatalf("partial failure lost applied changes: %q %+v %v", id, changes, err)
	}
	if changes.Upserts[0].Path != after.Path || changes.Removes[0].Path != second.Path {
		t.Fatalf("recovery did not reflect physical files: %+v", changes)
	}
	if err = partial.AcknowledgeAssetDownloadChanges(id); !errors.Is(err, ErrAssetApplyPending) {
		t.Fatalf("incomplete recovery was acknowledged: %v", err)
	}
	chunk, err := full.store.GetChunk(second.Chunks[0])
	if err != nil {
		t.Fatal(err)
	}
	if err = partial.store.PutChunk(chunk); err != nil {
		t.Fatal(err)
	}
	nextID, changes, err := partial.RecoverAssetDownloads(nil)
	if err != nil || nextID == "" || nextID == id || len(changes.Upserts) != 2 || len(changes.Removes) != 0 {
		t.Fatalf("retry lost recovery changes: %q %+v %v", nextID, changes, err)
	}
	if err = partial.ConfigureAssetDownloads(true, partial.assetDownloads.path, "test-scope"); err != nil {
		t.Fatal(err)
	}
	if got, _, readErr := partial.AssetDownloadChanges(); readErr != nil || got != nextID {
		t.Fatalf("reopening lost unacknowledged changes: %q %v", got, readErr)
	}
	if err = partial.AcknowledgeAssetDownloadChanges(id); !errors.Is(err, ErrAssetApplyPending) {
		t.Fatalf("stale acknowledgement discarded newer changes: %v", err)
	}
	if _, err = partial.HasIncompleteSnapshots(); !errors.Is(err, ErrAssetApplyPending) {
		t.Fatalf("source transition ignored unacknowledged recovery: %v", err)
	}
	if err = partial.AcknowledgeAssetDownloadChanges(nextID); err != nil {
		t.Fatal(err)
	}
	if got, changes, readErr := partial.AssetDownloadChanges(); readErr != nil || got != "" || changes.DataChanged() {
		t.Fatalf("acknowledged changes remain pending: %q %+v %v", got, changes, readErr)
	}
}
