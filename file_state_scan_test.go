package dejavu

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
)

func TestFileStateRejectsOmittedExistingSources(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	p := "/assets/probe.txt"
	writeAssetTestFile(t, repo, p, "preserved", 2)
	before, err := repo.Index("before", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	file := assetTestFile(t, repo, p)
	for _, full := range []bool{false, true} {
		_, removes, err := repo.diffLocalFiles(nil, []*entity.File{file}, nil, full)
		if !errors.Is(err, ErrIndexFileChanged) || len(removes) != 0 {
			t.Fatalf("omitted source accepted as removal, full=%v: %v %v", full, removes, err)
		}
	}
	after, err := repo.Latest()
	if err != nil || after.ID != before.ID || snapshotFileStateContent(t, repo, before, p) != "preserved" {
		t.Fatalf("failed scan changed snapshot: %v", err)
	}
	data, err := os.ReadFile(repo.absPath(p))
	if err != nil || string(data) != "preserved" {
		t.Fatalf("failed scan changed source: %v", err)
	}
}

func TestFileStateAllowsRealDeletionsAndExclusions(t *testing.T) {
	for _, mode := range []string{"deleted", "ignored", "directory"} {
		t.Run(mode, func(t *testing.T) {
			base := t.TempDir()
			repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
			p := "/assets/probe.txt"
			writeAssetTestFile(t, repo, p, "before", 2)
			if _, err := repo.Index("before", false, nil); err != nil {
				t.Fatal(err)
			}
			file := assetTestFile(t, repo, p)
			if mode == "ignored" {
				repo.IgnoreLines = append(repo.IgnoreLines, "assets/probe.txt")
			} else {
				if err := os.Remove(repo.absPath(p)); err != nil {
					t.Fatal(err)
				}
				if mode == "directory" {
					if err := os.Mkdir(repo.absPath(p), 0755); err != nil {
						t.Fatal(err)
					}
				}
			}
			_, removes, err := repo.diffLocalFiles(nil, []*entity.File{file}, nil, false)
			if err != nil || len(removes) != 1 || removes[0].ID != file.ID {
				t.Fatalf("intentional removal rejected: %v %v", removes, err)
			}
		})
	}
}
