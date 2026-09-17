package dejavu

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

func writeAppearanceTestPackage(t *testing.T, repo *Repo, key string, files map[string]string, tick int64, migration bool) {
	t.Helper()
	if err := os.RemoveAll(repo.absPath(key)); err != nil {
		t.Fatal(err)
	}
	digests := map[string]string{}
	for p, data := range files {
		writeAssetTestFile(t, repo, key+"/"+p, data, tick)
		digest := sha256.Sum256([]byte(data))
		digests[p] = hex.EncodeToString(digest[:])
	}
	record := map[string]interface{}{"version": 1, "deleted": len(files) == 0, "migration": migration,
		"installTime": 1, "updateTime": tick, "files": digests}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, repo, appearanceRecordPath(key), string(data), tick)
}

func readAppearanceTestFile(t *testing.T, repo *Repo, p string) string {
	t.Helper()
	data, err := os.ReadFile(repo.absPath(p))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func syncAppearanceTestRepo(t *testing.T, repo *Repo) *MergeResult {
	t.Helper()
	if repo.appearanceSyncEnabled {
		if data, err := os.ReadFile(repo.absPath("/.siyuan/syncignore")); err == nil {
			lines, parseErr := appearanceUserIgnoreLines(strings.Split(string(data), "\n"))
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			repo.appearanceIgnoreLines = lines
			repo.IgnoreLines = strings.Split(string(data), "\n")
		}
	}
	if _, err := repo.Index("test", true, nil); err != nil {
		t.Fatal(err)
	}
	result, _, err := repo.Sync(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := repo.AssetDownloadChanges()
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.AcknowledgeAssetDownloadChanges(id); err != nil {
		t.Fatal(err)
	}
	return result
}

func newAppearanceSyncTestRepo(t *testing.T, base, remote, device string, onDemand bool) *Repo {
	t.Helper()
	repo := newAssetTestRepo(t, base, remote, device, onDemand)
	repo.appearanceSyncEnabled = true
	return repo
}

func TestAppearancePackageKey(t *testing.T) {
	for p, expected := range map[string]string{
		"/themes/example/theme.css":              "/themes/example",
		"/themes/example/assets/font.woff":       "/themes/example",
		"/icons/example/icon.js":                 "/icons/example",
		"/storage/bazaar/themes/example.json":    "/themes/example",
		"/storage/bazaar/icons/example.json":     "/icons/example",
		"/storage/bazaar/themes/../example.json": "",
		"/themes/../example/theme.css":           "",
		"/themes/.staging/theme.css":             "",
		"/assets/themes/example.css":             "",
		"/storage/bazaar.json":                   "",
	} {
		if got := appearancePackageKey(p); got != expected {
			t.Errorf("%s: expected %q, got %q", p, expected, got)
		}
	}
}

func TestAppearancePackageDecisions(t *testing.T) {
	pkg := func(content string, deleted bool) *appearancePackage {
		p := &appearancePackage{Key: "/themes/test", Files: map[string]*entity.File{}, Record: &appearanceRecord{Version: 1, Deleted: deleted}}
		p.Files[appearanceRecordPath(p.Key)] = newTestSyncFileAtPath(appearanceRecordPath(p.Key), content, 0)
		if !deleted {
			p.Files[p.Key+"/theme.css"] = newTestSyncFileAtPath(p.Key+"/theme.css", content, 0)
		}
		return p
	}
	base, local, cloud, tombstone := pkg("base", false), pkg("local", false), pkg("cloud", false), pkg("deleted", true)
	tests := []struct {
		name               string
		base, local, cloud *appearancePackage
		winner             syncFileWinner
		conflict           ConflictType
	}{
		{"one local update", base, local, base, syncFileWinnerLocal, ""},
		{"one cloud update", base, base, cloud, syncFileWinnerCloud, ""},
		{"concurrent update", base, local, cloud, syncFileWinnerCloud, ConflictTypeLocalUpsertCloudUpsert},
		{"cloud deletion", base, local, tombstone, syncFileWinnerCloud, ConflictTypeLocalUpsertCloudRemove},
		{"local deletion", base, tombstone, cloud, syncFileWinnerLocal, ConflictTypeLocalRemoveCloudUpsert},
		{"late migration", nil, local, tombstone, syncFileWinnerCloud, ConflictTypeLocalUpsertCloudRemove},
		{"observed tombstone reinstall", tombstone, local, tombstone, syncFileWinnerLocal, ""},
		{"first concurrent install", nil, local, cloud, syncFileWinnerCloud, ConflictTypeLocalUpsertCloudUpsert},
		{"legacy local removal", base, nil, cloud, syncFileWinnerLocal, ConflictTypeLocalRemoveCloudUpsert},
		{"legacy cloud removal", base, local, nil, syncFileWinnerCloud, ConflictTypeLocalUpsertCloudRemove},
		{"cloud absence cannot erase tombstone", tombstone, tombstone, nil, syncFileWinnerLocal, ""},
		{"both remove retains tombstone", base, tombstone, nil, syncFileWinnerLocal, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := decideAppearancePackage(test.base, test.local, test.cloud)
			if decision.Winner != test.winner || decision.ConflictType != test.conflict {
				t.Fatalf("unexpected decision: %+v", decision)
			}
		})
	}
}

func TestAppearanceConcurrentUpdatesPreserveCompleteHistory(t *testing.T) {
	for _, onDemand := range []bool{false, true} {
		t.Run(map[bool]string{false: "full", true: "on demand"}[onDemand], func(t *testing.T) {
			base, remote := t.TempDir(), t.TempDir()
			a := newAppearanceSyncTestRepo(t, base, remote, "a", onDemand)
			b := newAppearanceSyncTestRepo(t, base, remote, "b", onDemand)
			key := "/themes/example"
			writeAppearanceTestPackage(t, a, key, map[string]string{"theme.json": "base manifest", "theme.css": "base css", "assets/font.woff": "base font"}, 10, false)
			syncAppearanceTestRepo(t, a)
			syncAppearanceTestRepo(t, b)
			writeAppearanceTestPackage(t, a, key, map[string]string{"theme.json": "cloud manifest", "theme.css": "cloud css", "assets/font.woff": "base font"}, 11, false)
			writeAppearanceTestPackage(t, b, key, map[string]string{"theme.json": "base manifest", "theme.css": "local css", "assets/font.woff": "local font"}, 11, false)
			syncAppearanceTestRepo(t, a)
			result := syncAppearanceTestRepo(t, b)
			if got := readAppearanceTestFile(t, b, key+"/theme.css"); got != "cloud css" {
				t.Fatalf("mixed package css: %s", got)
			}
			if got := readAppearanceTestFile(t, b, key+"/assets/font.woff"); got != "base font" {
				t.Fatalf("mixed package asset: %s", got)
			}
			if len(result.HistoryPaths) != 4 {
				t.Fatalf("complete losing package was not preserved: %v", result.HistoryPaths)
			}
			for _, relative := range []string{"theme.json", "theme.css", "assets/font.woff"} {
				history := filepath.Join(b.HistoryPath, result.Time.Format("2006-01-02-150405")+"-sync", key, relative)
				if _, err := os.Stat(history); err != nil {
					t.Fatalf("missing package history %s: %v", relative, err)
				}
			}
			before, err := b.Latest()
			if err != nil {
				t.Fatal(err)
			}
			syncAppearanceTestRepo(t, a)
			syncAppearanceTestRepo(t, b)
			after, err := b.Latest()
			if err != nil || before.ID != after.ID {
				t.Fatalf("sync did not converge: %s, %v, %v", before.ID, after, err)
			}
			deferred, err := b.DeferredAssets()
			if err != nil || len(deferred) != 0 {
				t.Fatalf("package assets were deferred: %v %v", deferred, err)
			}
		})
	}
}

func TestAppearanceDeletionMigrationAndReinstall(t *testing.T) {
	base, remote := t.TempDir(), t.TempDir()
	a := newAppearanceSyncTestRepo(t, base, remote, "a", false)
	b := newAppearanceSyncTestRepo(t, base, remote, "b", false)
	late := newAppearanceSyncTestRepo(t, base, remote, "late", false)
	key := "/icons/example"
	writeAppearanceTestPackage(t, a, key, map[string]string{"icon.json": "base manifest", "icon.js": "base icons"}, 10, false)
	syncAppearanceTestRepo(t, a)
	syncAppearanceTestRepo(t, b)
	writeAppearanceTestPackage(t, a, key, nil, 20, false)
	writeAppearanceTestPackage(t, b, key, map[string]string{"icon.json": "local manifest", "icon.js": "local icons"}, 30, false)
	syncAppearanceTestRepo(t, a)
	result := syncAppearanceTestRepo(t, b)
	if _, err := os.Stat(b.absPath(key + "/icon.js")); !errors.Is(err, os.ErrNotExist) || len(result.HistoryPaths) != 3 {
		t.Fatalf("cloud deletion must retain complete conflict history: %v %+v", err, result)
	}
	writeAppearanceTestPackage(t, late, key, map[string]string{"icon.json": "old manifest", "icon.js": "old icons"}, 100, true)
	syncAppearanceTestRepo(t, late)
	if _, err := os.Stat(late.absPath(key + "/icon.js")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late migration resurrected deleted package: %v", err)
	}
	writeAppearanceTestPackage(t, late, key, map[string]string{"icon.json": "reinstalled manifest", "icon.js": "reinstalled icons"}, 110, false)
	syncAppearanceTestRepo(t, late)
	syncAppearanceTestRepo(t, a)
	if got := readAppearanceTestFile(t, a, key+"/icon.js"); got != "reinstalled icons" {
		t.Fatalf("explicit reinstall after observing tombstone was lost: %s", got)
	}
}

func TestAppearanceRecordIntegrityAndPartialIgnore(t *testing.T) {
	base, remote := t.TempDir(), t.TempDir()
	repo := newAppearanceSyncTestRepo(t, base, remote, "a", false)
	key := "/themes/example"
	writeAppearanceTestPackage(t, repo, key, map[string]string{"theme.json": "manifest", "theme.css": "css"}, 10, false)
	if _, err := repo.Index("valid", true, nil); err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, repo, key+"/theme.css", "tampered", 11)
	if _, err := repo.Index("invalid", true, nil); err == nil {
		t.Fatalf("unrecorded payload change must fail: %v", err)
	}
	writeAppearanceTestPackage(t, repo, key, map[string]string{"theme.json": "manifest", "theme.css": "css"}, 20, false)
	repo.IgnoreLines = []string{"themes/example/theme.css"}
	repo.appearanceIgnoreLines = repo.IgnoreLines
	if _, err := repo.Index("partial ignore", true, nil); err == nil {
		t.Fatalf("partially ignored package must fail: %v", err)
	}
	if got := readAppearanceTestFile(t, repo, key+"/theme.css"); got != "css" {
		t.Fatalf("failed validation changed original: %s", got)
	}
	repo.IgnoreLines = []string{"storage/bazaar/themes/example.json"}
	repo.appearanceIgnoreLines = repo.IgnoreLines
	if _, err := repo.Index("ignored record", true, nil); err == nil {
		t.Fatalf("ignoring only package record must fail: %v", err)
	}
}

func TestAppearanceSameSecondAndPreservedTimestampUpdates(t *testing.T) {
	repo := newAppearanceSyncTestRepo(t, t.TempDir(), t.TempDir(), "a", false)
	key := "/themes/example"
	writeAppearanceTestPackage(t, repo, key, map[string]string{"theme.json": "manifest", "theme.css": "aaa"}, 10, false)
	first, err := repo.Index("first", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	writeAppearanceTestPackage(t, repo, key, map[string]string{"theme.json": "manifest", "theme.css": "bbb"}, 10, false)
	changed, err := repo.CheckSnapshot()
	if err != nil || !changed {
		t.Fatalf("same-time edit not detected: %v %v", changed, err)
	}
	second, err := repo.Index("second", true, nil)
	if err != nil || first.ID == second.ID {
		t.Fatalf("same-time edit not indexed: %v %v", second, err)
	}
	files, err := repo.GetFiles(second)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := repo.appearanceProjectionFiles(files, nil)
	if err != nil {
		t.Fatal(err)
	}
	file := projection[key+"/theme.css"]
	data, err := repo.openFile(file)
	if err != nil || string(data) != "bbb" {
		t.Fatalf("stale same-time contents indexed: %s %v", data, err)
	}
	changed, err = repo.CheckSnapshot()
	if err != nil || changed {
		t.Fatalf("unchanged content keeps changing snapshot: %v %v", changed, err)
	}
}

func TestAppearanceWholeIgnorePropagationPreservesLocalPackage(t *testing.T) {
	base, remote := t.TempDir(), t.TempDir()
	a := newAppearanceSyncTestRepo(t, base, remote, "a", false)
	b := newAppearanceSyncTestRepo(t, base, remote, "b", false)
	key := "/themes/example"
	writeAppearanceTestPackage(t, a, key, map[string]string{"theme.json": "manifest", "theme.css": "css"}, 10, false)
	syncAppearanceTestRepo(t, a)
	syncAppearanceTestRepo(t, b)
	previousFilter := a.pathFilter
	a.IgnoreLines = []string{"themes/example/"}
	a.pathFilter = func(info os.FileInfo, abs string) (bool, error) {
		if a.relPath(abs) == appearanceRecordPath(key) {
			return true, nil
		}
		return previousFilter(info, abs)
	}
	writeAssetTestFile(t, a, "/.siyuan/syncignore", "themes/example/", 20)
	syncAppearanceTestRepo(t, a)
	syncAppearanceTestRepo(t, b)
	for _, repo := range []*Repo{a, b} {
		if got := readAppearanceTestFile(t, repo, key+"/theme.css"); got != "css" {
			t.Fatalf("whole ignore removed local package on %s: %s", repo.DeviceID, got)
		}
		if _, err := os.Stat(repo.absPath(appearanceRecordPath(key))); err != nil {
			t.Fatalf("whole ignore removed local record on %s: %v", repo.DeviceID, err)
		}
		latest, err := repo.Latest()
		if err != nil {
			t.Fatal(err)
		}
		files, err := repo.GetFiles(latest)
		if err != nil || len(appearancePackages(files)) != 0 {
			t.Fatalf("whole ignored package stayed in snapshot: %v %v", files, err)
		}
	}
}

func TestAppearanceManualDownloadWholeIgnorePreservesLocalPackage(t *testing.T) {
	base, remote := t.TempDir(), t.TempDir()
	a := newAppearanceSyncTestRepo(t, base, remote, "a", false)
	b := newAppearanceSyncTestRepo(t, base, remote, "b", false)
	key := "/themes/example"
	writeAppearanceTestPackage(t, a, key, map[string]string{"theme.json": "manifest", "theme.css": "css"}, 10, false)
	syncAppearanceTestRepo(t, a)
	syncAppearanceTestRepo(t, b)
	previousFilter := a.pathFilter
	a.IgnoreLines = []string{"themes/example/"}
	a.pathFilter = func(info os.FileInfo, abs string) (bool, error) {
		if a.relPath(abs) == appearanceRecordPath(key) {
			return true, nil
		}
		return previousFilter(info, abs)
	}
	writeAssetTestFile(t, a, "/.siyuan/syncignore", "themes/example/", 20)
	syncAppearanceTestRepo(t, a)
	if _, _, err := b.SyncDownload(nil); err != nil {
		t.Fatal(err)
	}
	if got := readAppearanceTestFile(t, b, key+"/theme.css"); got != "css" {
		t.Fatalf("manual download removed ignored local package: %s", got)
	}
	if _, err := os.Stat(b.absPath(appearanceRecordPath(key))); err != nil {
		t.Fatalf("manual download removed ignored local state: %v", err)
	}
}

func TestAppearanceLegacyFileIDsRemainReadableAndConverge(t *testing.T) {
	base, remote := t.TempDir(), t.TempDir()
	a := newAssetTestRepo(t, base, remote, "a", false)
	b := newAssetTestRepo(t, base, remote, "b", false)
	key := "/themes/legacy"
	writeAssetTestFile(t, a, key+"/theme.css", "legacy css", 10)
	writeAssetTestFile(t, a, key+"/theme.json", "legacy manifest", 10)
	index, err := a.Index("current", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	files, err := a.GetFiles(index)
	if err != nil {
		t.Fatal(err)
	}
	legacy := *index
	legacy.ID = util.RandHash()
	legacy.Files = nil
	for _, file := range files {
		if appearancePackageKey(file.Path) != "" {
			old := *file
			old.ID = entity.NewFile(file.Path, file.Size, file.Updated).ID
			file = &old
			if err = a.store.PutFile(file); err != nil {
				t.Fatal(err)
			}
		}
		legacy.Files = append(legacy.Files, file.ID)
	}
	if err = a.UpdateLatest(&legacy); err != nil {
		t.Fatal(err)
	}
	if _, err = a.SyncUpload(nil); err != nil {
		t.Fatal(err)
	}
	syncAppearanceTestRepo(t, b)
	if got := readAppearanceTestFile(t, b, key+"/theme.css"); got != "legacy css" {
		t.Fatalf("legacy package could not be read: %s", got)
	}
	syncAppearanceTestRepo(t, b)
	syncAppearanceTestRepo(t, a)
	stable, err := b.Latest()
	if err != nil {
		t.Fatal(err)
	}
	syncAppearanceTestRepo(t, b)
	after, err := b.Latest()
	if err != nil || stable.ID != after.ID {
		t.Fatalf("legacy identity migration did not converge: %v %v", after, err)
	}
}

func TestAppearanceRejectsLegacyMixedCloudPackage(t *testing.T) {
	base, remote := t.TempDir(), t.TempDir()
	a := newAssetTestRepo(t, base, remote, "a", false)
	b := newAssetTestRepo(t, base, remote, "b", false)
	key := "/themes/example"
	writeAppearanceTestPackage(t, a, key, map[string]string{"theme.json": "old manifest", "theme.css": "old css"}, 10, false)
	syncAppearanceTestRepo(t, a)
	syncAppearanceTestRepo(t, b)
	oldRecord := assetTestFile(t, a, appearanceRecordPath(key))
	writeAppearanceTestPackage(t, a, key, map[string]string{"theme.json": "new manifest", "theme.css": "new css"}, 20, false)
	syncAppearanceTestRepo(t, a)
	latest, err := a.Latest()
	if err != nil {
		t.Fatal(err)
	}
	files, err := a.getFiles(latest.Files)
	if err != nil {
		t.Fatal(err)
	}
	mixed := &entity.Index{ID: util.RandHash(), Created: time.Now().UnixMilli()}
	mixed.InitAESKeyVerifyVal(a.store.AesKey)
	for _, file := range files {
		if file.Path == oldRecord.Path {
			file = oldRecord
		}
		mixed.Files = append(mixed.Files, file.ID)
		mixed.Size += file.Size
	}
	mixed.Count = len(mixed.Files)
	if err = a.UpdateLatest(mixed); err != nil {
		t.Fatal(err)
	}
	if err = a.updateCloudIndexes(mixed, &TrafficStat{m: &sync.Mutex{}}, nil); err != nil {
		t.Fatal(err)
	}
	b.appearanceSyncEnabled = true
	if _, _, err = b.Sync(nil); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("mixed cloud package must be rejected: %v", err)
	}
	if got := readAppearanceTestFile(t, b, key+"/theme.css"); got != "old css" {
		t.Fatalf("invalid cloud package replaced local data: %s", got)
	}
}
