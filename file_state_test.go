package dejavu

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/restic/chunker"
	"github.com/siyuan-note/dejavu/entity"
)

func writeFileStateTest(t *testing.T, repo *Repo, p, content string, stamp time.Time) {
	t.Helper()
	abs := repo.absPath(p)
	if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(abs, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(abs)
	if err != nil || info.ModTime().UnixNano() != stamp.UnixNano() {
		t.Fatalf("file timestamp setup failed: %v %v", info, err)
	}
}

func snapshotFileStateContent(t *testing.T, repo *Repo, index *entity.Index, p string) string {
	t.Helper()
	files, err := repo.GetFiles(index)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.Path == p {
			data, err := repo.OpenFile(file)
			if err != nil {
				t.Fatal(err)
			}
			return string(data)
		}
	}
	t.Fatalf("file absent from snapshot: %s", p)
	return ""
}

func TestFileStateSameSecondChanges(t *testing.T) {
	for _, tc := range []struct {
		name, content string
		nanos         int64
	}{
		{"different-size", "after-content", 900000000},
		{"equal-size", "AFTER!", 900000000},
		{"sub-millisecond", "AFTER!", 100000900},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
			p := "/assets/probe.txt"
			stamp := time.Unix(1700000100, 100000100)
			writeFileStateTest(t, repo, p, "before", stamp)
			before, err := repo.Index("before", true, nil)
			if err != nil {
				t.Fatal(err)
			}
			writeFileStateTest(t, repo, p, tc.content, time.Unix(stamp.Unix(), tc.nanos))
			changed, err := repo.CheckSnapshot()
			if err != nil || !changed {
				t.Fatalf("precheck missed same-second edit: %v", err)
			}
			after, created, err := repo.IndexWithResult("after", true, nil)
			if err != nil || !created || after.ID == before.ID {
				t.Fatalf("index missed edit: %v", err)
			}
			if snapshotFileStateContent(t, repo, after, p) != tc.content || snapshotFileStateContent(t, repo, before, p) != "before" {
				t.Fatal("new or historical snapshot content is incorrect")
			}
			stable, created, err := repo.IndexWithResult("stable", true, nil)
			if err != nil || created || stable.ID != after.ID {
				t.Fatalf("index did not converge: %v", err)
			}
		})
	}
}

func TestFileStateFullCheckAndKnownWrites(t *testing.T) {
	for _, knownWrite := range []bool{false, true} {
		t.Run(map[bool]string{false: "external-full-check", true: "known-write"}[knownWrite], func(t *testing.T) {
			base := t.TempDir()
			repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
			p := "/assets/probe.txt"
			stamp := time.Unix(1700000100, 100000000)
			writeFileStateTest(t, repo, p, "before", stamp)
			before, err := repo.Index("before", true, nil)
			if err != nil {
				t.Fatal(err)
			}
			writeFileStateTest(t, repo, p, "AFTER!", stamp)
			if knownWrite {
				MarkFileChanged(repo.absPath(p))
			} else {
				unchanged, err := repo.Index("fast", true, nil)
				if err != nil || unchanged.ID != before.ID {
					t.Fatalf("fast path unexpectedly read unchanged attributes: %v", err)
				}
			}
			after, created, err := repo.IndexWithResult("verified", true, nil, !knownWrite)
			if err != nil || !created || after.ID == before.ID || snapshotFileStateContent(t, repo, after, p) != "AFTER!" {
				t.Fatalf("content change not detected: %v", err)
			}
			stable, created, err := repo.IndexWithResult("unchanged full", true, nil, true)
			if err != nil || created || stable.ID != after.ID {
				t.Fatalf("unchanged full check made a version: %v", err)
			}
			if snapshotFileStateContent(t, repo, before, p) != "before" {
				t.Fatal("old snapshot overwritten")
			}
		})
	}
}

func TestFileStateCheckoutVerifiesContent(t *testing.T) {
	for _, content := range []string{"after-content", "AFTER!"} {
		for _, sameStamp := range []bool{false, true} {
			t.Run(content+map[bool]string{false: "-same-second", true: "-same-time"}[sameStamp], func(t *testing.T) {
				base := t.TempDir()
				repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
				p := "/assets/probe.txt"
				stamp := time.Unix(1700000100, 100000000)
				writeFileStateTest(t, repo, p, "before", stamp)
				before, err := repo.Index("before", true, nil)
				if err != nil {
					t.Fatal(err)
				}
				modified := stamp
				if !sameStamp {
					modified = stamp.Add(800 * time.Millisecond)
				}
				writeFileStateTest(t, repo, p, content, modified)
				upserts, _, err := repo.Checkout(before.ID, nil)
				if err != nil || len(upserts) != 1 {
					t.Fatalf("checkout skipped changed content: %v", err)
				}
				data, err := os.ReadFile(repo.absPath(p))
				if err != nil || string(data) != "before" {
					t.Fatalf("wrong restored content %q: %v", data, err)
				}
				stable, created, err := repo.IndexWithResult("after checkout", true, nil)
				if err != nil || created || stable.ID != before.ID {
					t.Fatalf("restored file reindexed: %v", err)
				}
			})
		}
	}
}

func TestFileStateCacheRecoveryAndReadOnlyCheck(t *testing.T) {
	for _, damage := range []string{"missing", "corrupt", "version", "index", "device", "path"} {
		t.Run(damage, func(t *testing.T) {
			base := t.TempDir()
			repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
			p := "/assets/probe.txt"
			stamp := time.Unix(1700000100, 100000000)
			writeFileStateTest(t, repo, p, "before", stamp)
			before, err := repo.Index("before", true, nil)
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(repo.fileStatePath())
			if err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "missing":
				err = os.Remove(repo.fileStatePath())
			case "corrupt":
				err = os.WriteFile(repo.fileStatePath(), []byte("invalid"), 0600)
			default:
				var state localFileState
				if err = json.Unmarshal(data, &state); err != nil {
					t.Fatal(err)
				}
				switch damage {
				case "version":
					state.Version++
				case "index":
					state.IndexID = strings.Repeat("0", 40)
				case "device":
					state.DeviceID = "other"
				case "path":
					state.DataPath = "other"
				}
				data, err = json.Marshal(state)
				if err == nil {
					err = os.WriteFile(repo.fileStatePath(), data, 0600)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			writeFileStateTest(t, repo, p, "AFTER!", stamp)
			original := snapshotTestFiles(t, repo.Path)
			changed, err := repo.CheckSnapshot()
			if err != nil || !changed {
				t.Fatalf("invalid cache hid a change: %v", err)
			}
			for name, data := range snapshotTestFiles(t, repo.Path) {
				if original[name] != data {
					t.Fatalf("precheck wrote %s", name)
				}
			}
			after, err := repo.Index("recovered", true, nil)
			if err != nil || after.ID == before.ID || snapshotFileStateContent(t, repo, after, p) != "AFTER!" {
				t.Fatalf("rebuild failed: %v", err)
			}
		})
	}
}

func TestFileStateColdReuseAndTimePrecision(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	p := "/assets/probe.txt"
	stamp := time.Unix(1700000100, 100000000)
	writeFileStateTest(t, repo, p, "before", stamp)
	before, err := repo.Index("before", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	writeFileStateTest(t, repo, p, "before", stamp.Add(123400*time.Nanosecond))
	stable, created, err := repo.IndexWithResult("precision only", true, nil)
	if err != nil || created || stable.ID != before.ID {
		t.Fatalf("precision drift made a snapshot: %v", err)
	}
	stateBefore, err := os.ReadFile(repo.fileStatePath())
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewRepo(repo.DataPath, repo.Path, repo.HistoryPath, repo.TempPath, repo.DeviceID, repo.DeviceName, repo.DeviceOS, repo.store.AesKey, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	stable, err = other.Index("restart", true, nil)
	if err != nil || stable.ID != before.ID {
		t.Fatalf("cold index changed snapshot: %v", err)
	}
	stateAfter, err := os.ReadFile(repo.fileStatePath())
	if err != nil || !bytes.Equal(stateBefore, stateAfter) {
		t.Fatalf("unchanged observations rewritten: %v", err)
	}
}

func TestFileStateSyncAndLocalCache(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	first := newAssetTestRepo(t, base, remote, "first", false)
	p := "/assets/probe.txt"
	stamp := time.Unix(1700000100, 100000000)
	writeFileStateTest(t, first, p, "before", stamp)
	syncAssetTestRepo(t, first)
	writeFileStateTest(t, first, p, "AFTER!", stamp.Add(800*time.Millisecond))
	syncAssetTestRepo(t, first)
	second := newAssetTestRepo(t, base, remote, "second", false)
	syncAssetTestRepo(t, second)
	data, err := os.ReadFile(second.absPath(p))
	if err != nil || string(data) != "AFTER!" {
		t.Fatalf("new content not synced: %q %v", data, err)
	}
	latest, err := second.Latest()
	if err != nil {
		t.Fatal(err)
	}
	stable, err := second.Index("stable after download", true, nil)
	if err != nil || stable.ID != latest.ID {
		t.Fatalf("download reindexed: %v", err)
	}
	if _, err = os.Stat(filepath.Join(remote, "main", "file-state-v1.json")); !os.IsNotExist(err) {
		t.Fatalf("local cache uploaded: %v", err)
	}
}

func TestFileStateLargeFileChunkReuse(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	p := "/assets/large.bin"
	stamp := time.Unix(1700000100, 100000000)
	content := bytes.Repeat([]byte("abcdefg"), chunker.MaxSize)
	writeFileStateTest(t, repo, p, string(content), stamp)
	before, err := repo.Index("before", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	previous := assetTestFile(t, repo, p)
	content[len(content)-1] = 'X'
	writeFileStateTest(t, repo, p, string(content), stamp.Add(800*time.Millisecond))
	after, err := repo.Index("after", true, nil)
	if err != nil || after.ID == before.ID {
		t.Fatalf("large edit missed: %v", err)
	}
	current := assetTestFile(t, repo, p)
	if len(previous.Chunks) < 2 || previous.Chunks[0] != current.Chunks[0] || previous.Chunks[len(previous.Chunks)-1] == current.Chunks[len(current.Chunks)-1] {
		t.Fatal("unchanged chunks not reused or changed chunk lost")
	}
}
