package dejavu

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/restic/chunker"
	"github.com/siyuan-note/dejavu/entity"
)

// 原有方法签名仍可满足调用方的接口声明。
var _ interface {
	Index(string, bool, map[string]interface{}) (*entity.Index, error)
	IndexWithResult(string, bool, map[string]interface{}) (*entity.Index, bool, error)
} = (*Repo)(nil)

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
			after, created, err := repo.IndexWithResultOptions("verified", true, nil, IndexOptions{FullCheck: !knownWrite})
			if err != nil || !created || after.ID == before.ID || snapshotFileStateContent(t, repo, after, p) != "AFTER!" {
				t.Fatalf("content change not detected: %v", err)
			}
			stable, created, err := repo.IndexWithResultOptions("unchanged full", true, nil, IndexOptions{FullCheck: true})
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

func TestFileStateCacheReadFailureFallsBackToContent(t *testing.T) {
	for _, fullCheck := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "full"}[fullCheck], func(t *testing.T) {
			base := t.TempDir()
			repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
			p := "/assets/probe.txt"
			stamp := time.Unix(1700000100, 100000000)
			writeFileStateTest(t, repo, p, "before", stamp)
			before, err := repo.Index("before", true, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.Remove(repo.fileStatePath()); err != nil {
				t.Fatal(err)
			}
			if err = os.Mkdir(repo.fileStatePath(), 0700); err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(repo.fileStatePath(), "keep")
			if err = os.WriteFile(sentinel, []byte("preserve"), 0600); err != nil {
				t.Fatal(err)
			}
			if changed, checkErr := repo.CheckSnapshot(); checkErr != nil || changed {
				t.Fatalf("cache failure blocked unchanged precheck: changed=%v err=%v", changed, checkErr)
			}
			writeFileStateTest(t, repo, p, "AFTER!", stamp)
			if changed, checkErr := repo.CheckSnapshot(); checkErr != nil || !changed {
				t.Fatalf("cache failure hid changed content: changed=%v err=%v", changed, checkErr)
			}
			after, created, err := repo.IndexWithResultOptions("after", true, nil, IndexOptions{FullCheck: fullCheck})
			if err != nil || !created || after.ID == before.ID {
				t.Fatalf("cache failure blocked indexing: created=%v err=%v", created, err)
			}
			if content := snapshotFileStateContent(t, repo, after, p); content != "AFTER!" {
				t.Fatalf("wrong indexed content: %q", content)
			}
			if _, _, err = repo.Checkout(before.ID, nil); err != nil {
				t.Fatalf("cache failure blocked restoration: %v", err)
			}
			if content, readErr := os.ReadFile(repo.absPath(p)); readErr != nil || string(content) != "before" {
				t.Fatalf("wrong restored content: %q %v", content, readErr)
			}
			if content, readErr := os.ReadFile(sentinel); readErr != nil || string(content) != "preserve" {
				t.Fatalf("unreadable cache path was modified: %q %v", content, readErr)
			}
		})
	}
}

func TestFileStateCacheWriteFailureKeepsSnapshotUsable(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows read-only files reject replacement without changing repository permissions")
	}
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	p := "/assets/probe.txt"
	stamp := time.Unix(1700000100, 100000000)
	writeFileStateTest(t, repo, p, "before", stamp)
	before, err := repo.Index("before", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	cached, err := os.ReadFile(repo.fileStatePath())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(repo.fileStatePath(), 0400); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(repo.fileStatePath(), 0600) })
	writeFileStateTest(t, repo, p, "AFTER!", stamp)
	MarkFileChanged(repo.absPath(p))
	after, created, err := repo.IndexWithResult("after", true, nil)
	if err != nil || !created || after.ID == before.ID || snapshotFileStateContent(t, repo, after, p) != "AFTER!" {
		t.Fatalf("cache write failure invalidated the snapshot: created=%v err=%v", created, err)
	}
	if content, readErr := os.ReadFile(repo.fileStatePath()); readErr != nil || !bytes.Equal(content, cached) {
		t.Fatalf("read-only cache was changed: %v", readErr)
	}
	if _, ok := fileWrites.Load(filepath.Clean(repo.absPath(p))); !ok {
		t.Fatal("failed cache persistence discarded the write mark")
	}
	other, err := NewRepo(repo.DataPath, repo.Path, repo.HistoryPath, repo.TempPath,
		repo.DeviceID, repo.DeviceName, repo.DeviceOS, repo.store.AesKey, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	writeFileStateTest(t, other, p, "LATER!", stamp)
	if changed, checkErr := other.CheckSnapshot(); checkErr != nil || !changed {
		t.Fatalf("stale persisted cache hid changed content: changed=%v err=%v", changed, checkErr)
	}
	if _, _, err = other.Checkout(before.ID, nil); err != nil {
		t.Fatalf("cache write failure blocked restoration: %v", err)
	}
	if content, readErr := os.ReadFile(other.absPath(p)); readErr != nil || string(content) != "before" {
		t.Fatalf("wrong restored content: %q %v", content, readErr)
	}
	if temporary, globErr := filepath.Glob(repo.fileStatePath() + ".*.tmp"); globErr != nil || len(temporary) != 0 {
		t.Fatalf("cache write failures left temporary files: %v %v", temporary, globErr)
	}
}

func TestFileStateCacheWriteCleansTemporaryOnFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file-state-v1.json")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeFileState(path, []byte("cache")); err == nil {
		t.Fatal("cache unexpectedly replaced an existing directory")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "file-state-v1.json" || !entries[0].IsDir() {
		t.Fatalf("failed cache replacement left temporary files or changed original data: %v %v", entries, err)
	}
}

func TestFileStateCacheFailurePreservesAuthenticationErrors(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	file := assetTestFile(t, repo, "/seed.txt")
	latest, err := repo.Latest()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(repo.fileStatePath()); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(repo.fileStatePath(), 0700); err != nil {
		t.Fatal(err)
	}
	_, chunkPath := repo.store.AbsPath(file.Chunks[0])
	encoded, err := os.ReadFile(chunkPath)
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] ^= 1
	if err = os.WriteFile(chunkPath, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(repo.absPath(file.Path), []byte("current content"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, _, err = repo.Checkout(latest.ID, nil); err == nil {
		t.Fatal("cache fallback bypassed snapshot authentication")
	}
	if content, readErr := os.ReadFile(repo.absPath(file.Path)); readErr != nil || string(content) != "current content" {
		t.Fatalf("authentication failure changed current content: %q %v", content, readErr)
	}
	if content, readErr := os.ReadFile(chunkPath); readErr != nil || !bytes.Equal(content, encoded) {
		t.Fatalf("authentication failure changed original ciphertext: %v", readErr)
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
