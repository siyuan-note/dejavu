package dejavu

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotScanRetryClassification(t *testing.T) {
	var previous *snapshotScanOmission
	first := &snapshotScanOmission{path: "/assets/file", size: 3, modified: 4, detail: "unknown", retryable: true}
	if stopSnapshotScanRetry(first, &previous) || !errors.Is(first, ErrIndexFileChanged) || !errors.Is(first, ErrSnapshotScanIncomplete) {
		t.Fatal("unknown omission must allow a rescan")
	}
	repeated := &snapshotScanOmission{path: first.path, size: first.size, modified: first.modified, detail: first.detail, retryable: true}
	if !stopSnapshotScanRetry(fmt.Errorf("index: %w", repeated), &previous) || errors.Is(repeated, ErrIndexFileChanged) {
		t.Fatal("repeated omission must not trigger another repository or kernel retry")
	}
	changed := &snapshotScanOmission{path: first.path, size: 5, modified: first.modified, detail: first.detail, retryable: true}
	if stopSnapshotScanRetry(changed, &previous) {
		t.Fatal("changing source must remain retryable")
	}
	if stopSnapshotScanRetry(ErrIndexFileChanged, &previous) || previous != nil {
		t.Fatal("ordinary file changes must keep their retry policy and clear omission history")
	}
	known := &snapshotScanOmission{path: first.path, detail: "ancestor link", retryable: false}
	if !stopSnapshotScanRetry(known, &previous) || errors.Is(known, ErrIndexFileChanged) {
		t.Fatal("known incomplete scan must fail without retries")
	}
}

func TestSnapshotScanUnknownOmissionRetryPreservesSnapshot(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprint(persistent), func(t *testing.T) {
			base := t.TempDir()
			repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
			p := "/assets/probe.txt"
			writeAssetTestFile(t, repo, p, "preserved", 2)
			writeAssetTestFile(t, repo, "/assets/other.txt", "other", 2)
			before, err := repo.Index("before", false, nil)
			if err != nil {
				t.Fatal(err)
			}
			rounds, visits := 0, 0
			repo.pathFilter = func(info os.FileInfo, path string) (bool, error) {
				if filepath.Clean(path) == filepath.Clean(repo.DataPath) {
					rounds++
				}
				if path == repo.absPath(p) {
					visits++
					// 首次访问来自遍历，第二次来自删除保护的复查。
					if visits%2 == 1 && (persistent || rounds == 1) {
						return true, nil
					}
				}
				return info.IsDir(), nil
			}
			_, err = repo.Index("retry", false, nil)
			if rounds != 2 {
				t.Fatalf("scan count %d, want 2", rounds)
			}
			if persistent {
				if !errors.Is(err, ErrSnapshotScanIncomplete) || errors.Is(err, ErrIndexFileChanged) {
					t.Fatalf("%v", err)
				}
				after, latestErr := repo.Latest()
				if latestErr != nil || after.ID != before.ID {
					t.Fatalf("snapshot changed: %v", latestErr)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if snapshotFileStateContent(t, repo, before, p) != "preserved" {
				t.Fatal("lost source")
			}
		})
	}
}
