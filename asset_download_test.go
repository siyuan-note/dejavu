package dejavu

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
)

func newAssetTestRepo(t *testing.T, base, remote, device string, onDemand bool) *Repo {
	t.Helper()
	dir := filepath.Join(base, device)
	repoPath := filepath.Join(dir, "repo")
	backend := cloud.NewLocal(&cloud.BaseCloud{Conf: &cloud.Conf{Dir: "main", RepoPath: repoPath,
		AvailableSize: 1024 * 1024 * 1024, Local: &cloud.ConfLocal{Endpoint: remote}}})
	repo, err := NewRepo(filepath.Join(dir, "data"), repoPath, filepath.Join(dir, "history"), filepath.Join(dir, "temp"),
		device, device, "windows", []byte("0123456789abcdef0123456789abcdef"), nil, backend)
	if err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, repo, "/seed.txt", "seed", 1)
	if err = repo.ConfigureAssetDownloads(onDemand, filepath.Join(dir, "conf", "assets"), "test-scope"); err != nil {
		t.Fatal(err)
	}
	if _, err = repo.Index("seed", false, nil); err != nil {
		t.Fatal(err)
	}
	return repo
}

func writeAssetTestFile(t *testing.T, repo *Repo, p, data string, tick int64) {
	t.Helper()
	abs := repo.absPath(p)
	if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(1700000000+tick, 0)
	if err := os.Chtimes(abs, stamp, stamp); err != nil {
		t.Fatal(err)
	}
}

func syncAssetTestRepo(t *testing.T, repo *Repo) {
	t.Helper()
	if _, err := repo.Index("test", true, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.Sync(nil); err != nil {
		t.Fatal(err)
	}
	id, _, err := repo.AssetDownloadChanges()
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.AcknowledgeAssetDownloadChanges(id); err != nil {
		t.Fatal(err)
	}
}

func assetTestFile(t *testing.T, repo *Repo, p string) *entity.File {
	t.Helper()
	index, err := repo.Latest()
	if err != nil {
		t.Fatal(err)
	}
	files, err := repo.GetFiles(index)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if f.Path == p {
			return f
		}
	}
	t.Fatalf("file absent from logical index: %s", p)
	return nil
}

func TestAssetDownloadsSyncAndHydrate(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	full := newAssetTestRepo(t, base, remote, "full", false)
	writeAssetTestFile(t, full, "/assets/test.bin", "version one", 2)
	writeAssetTestFile(t, full, "/book/doc/assets/nested.bin", "nested data", 2)
	writeAssetTestFile(t, full, "/assets/ocr-texts.json", "{}", 2)
	writeAssetTestFile(t, full, "/assets/test.sya", "annotations", 2)
	syncAssetTestRepo(t, full)
	partial := newAssetTestRepo(t, base, remote, "partial", true)
	syncAssetTestRepo(t, partial)
	deferred, err := partial.DeferredAssets()
	if err != nil || len(deferred) != 2 {
		t.Fatalf("deferred=%v err=%v", deferred, err)
	}
	for _, p := range []string{"/assets/test.bin", "/book/doc/assets/nested.bin"} {
		if _, err = os.Stat(partial.absPath(p)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unexpected local asset: %s, %v", p, err)
		}
		for _, chunk := range assetTestFile(t, partial, p).Chunks {
			if _, err = partial.store.Stat(chunk); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unexpected local chunk: %s, %v", chunk, err)
			}
		}
	}
	for _, p := range []string{"/assets/ocr-texts.json", "/assets/test.sya"} {
		if _, err = os.Stat(partial.absPath(p)); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.Remove(filepath.Join(partial.Path, "full-latest.json")); err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, partial, "/local.txt", "local document change", 3)
	syncAssetTestRepo(t, partial)
	if err = partial.ConfigureAssetDownloads(true, partial.assetDownloads.path, "test-scope"); err != nil {
		t.Fatal(err)
	}
	syncAssetTestRepo(t, full)
	writeAssetTestFile(t, full, "/assets/test.bin", "version two", 4)
	syncAssetTestRepo(t, full)
	syncAssetTestRepo(t, partial)
	if got := assetTestFile(t, partial, "/assets/test.bin"); got.Updated != time.Unix(1700000004, 0).UnixMilli() {
		t.Fatalf("old logical version: %+v", got)
	}
	if downloaded, ensureErr := partial.EnsureAsset("assets/test.bin", nil); ensureErr != nil || !downloaded {
		t.Fatalf("hydrate=%v err=%v", downloaded, ensureErr)
	}
	if data, readErr := os.ReadFile(partial.absPath("/assets/test.bin")); readErr != nil || string(data) != "version two" {
		t.Fatalf("data=%q err=%v", data, readErr)
	}
	writeAssetTestFile(t, full, "/assets/test.bin", "version three", 5)
	syncAssetTestRepo(t, full)
	syncAssetTestRepo(t, partial)
	if data, readErr := os.ReadFile(partial.absPath("/assets/test.bin")); readErr != nil || string(data) != "version three" {
		t.Fatalf("downloaded resource was not updated: %q %v", data, readErr)
	}
	if err = partial.ConfigureAssetDownloads(false, partial.assetDownloads.path, "test-scope"); err != nil {
		t.Fatal(err)
	}
	syncAssetTestRepo(t, partial)
	deferred, err = partial.DeferredAssets()
	if err != nil || len(deferred) != 0 {
		t.Fatalf("full mode incomplete: %v %v", deferred, err)
	}
}

func TestAssetDownloadsDeletionAndManualSync(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	full := newAssetTestRepo(t, base, remote, "full", false)
	writeAssetTestFile(t, full, "/assets/test.bin", "remote resource", 10)
	syncAssetTestRepo(t, full)
	partial := newAssetTestRepo(t, base, remote, "partial", true)
	if _, _, err := partial.SyncDownload(nil); err != nil {
		t.Fatal(err)
	}
	if deferred, err := partial.DeferredAssets(); err != nil || len(deferred) != 1 {
		t.Fatalf("manual download did not defer: %v %v", deferred, err)
	}
	if err := os.Remove(full.absPath("/assets/test.bin")); err != nil {
		t.Fatal(err)
	}
	syncAssetTestRepo(t, full)
	syncAssetTestRepo(t, partial)
	if deferred, err := partial.DeferredAssets(); err != nil || len(deferred) != 0 {
		t.Fatalf("deleted resource retained: %v %v", deferred, err)
	}
	writeAssetTestFile(t, full, "/assets/second.bin", "another remote resource", 11)
	syncAssetTestRepo(t, full)
	syncAssetTestRepo(t, partial)
	writeAssetTestFile(t, partial, "/local.txt", "manual upload", 12)
	if _, err := partial.Index("upload", true, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := partial.SyncUpload(nil); err != nil {
		t.Fatal(err)
	}
	if deferred, err := partial.DeferredAssets(); err != nil || len(deferred) != 1 {
		t.Fatalf("manual upload eagerly materialized a resource: %v %v", deferred, err)
	}
	if _, err := os.Stat(partial.absPath("/assets/second.bin")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manual upload checked out an asset: %v", err)
	}
}

func TestAssetDownloadsFullSnapshotAndStateIntegrity(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	full := newAssetTestRepo(t, base, remote, "full", false)
	writeAssetTestFile(t, full, "/assets/test.bin", "snapshot resource", 20)
	syncAssetTestRepo(t, full)
	partial := newAssetTestRepo(t, base, remote, "partial", true)
	syncAssetTestRepo(t, partial)
	index, err := partial.Latest()
	if err != nil {
		t.Fatal(err)
	}
	if _, chunks, _, downloadErr := partial.DownloadIndex(index.ID, nil); downloadErr != nil || chunks == 0 {
		t.Fatalf("existing metadata prevented full snapshot: chunks=%d err=%v", chunks, downloadErr)
	}
	f := assetTestFile(t, partial, "/assets/test.bin")
	if _, err = partial.OpenFile(f); err != nil {
		t.Fatal(err)
	}
	statePath := partial.assetDownloads.path
	if read, readErr := ReadDeferredAssets(statePath, partial.store.AesKey); readErr != nil || len(read) != 1 {
		t.Fatalf("readonly state=%v err=%v", read, readErr)
	}
	if err = partial.ConfigureAssetDownloads(true, statePath, "different-scope"); !errors.Is(err, ErrAssetDownloadState) {
		t.Fatalf("scope change accepted: %v", err)
	}
	state, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	state[len(state)-1] ^= 1
	if err = os.WriteFile(statePath, state, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = ReadDeferredAssets(statePath, partial.store.AesKey); !errors.Is(err, ErrAssetDownloadState) {
		t.Fatalf("corrupt state accepted: %v", err)
	}
	if err = os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	if err = partial.ConfigureAssetDownloads(true, statePath, "test-scope"); !errors.Is(err, ErrAssetDownloadState) {
		t.Fatalf("missing state accepted: %v", err)
	}
}

func TestAssetDownloadsConcurrentHydrationAndLocalWrite(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	full := newAssetTestRepo(t, base, remote, "full", false)
	writeAssetTestFile(t, full, "/assets/test.bin", strings.Repeat("asset", 1000), 30)
	syncAssetTestRepo(t, full)
	partial := newAssetTestRepo(t, base, remote, "partial", true)
	syncAssetTestRepo(t, partial)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := partial.EnsureAsset("assets/test.bin", nil)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	writeAssetTestFile(t, full, "/assets/new.bin", "remote", 31)
	syncAssetTestRepo(t, full)
	syncAssetTestRepo(t, partial)
	writeAssetTestFile(t, partial, "/assets/new.bin", "local replacement", 32)
	if _, err := partial.EnsureAsset("assets/new.bin", nil); !errors.Is(err, ErrIndexFileChanged) {
		t.Fatalf("local replacement overwritten: %v", err)
	}
	if err := os.Remove(filepath.Join(partial.Path, "full-latest.json")); err != nil {
		t.Fatal(err)
	}
	syncAssetTestRepo(t, partial)
	if data, err := os.ReadFile(partial.absPath("/assets/new.bin")); err != nil || string(data) != "local replacement" {
		t.Fatalf("local data lost: %q %v", data, err)
	}
}

func TestAssetDownloadPath(t *testing.T) {
	for _, p := range []string{"assets/a.png", "/assets/sub/a.pdf", "/book/doc/assets/a.bin"} {
		if !IsAssetDownloadPath(p) {
			t.Fatal(p)
		}
	}
	for _, p := range []string{"/assets/ocr-texts.json", "/assets/a.sya", "/assets/.metadata", "/assets/../note.sy", "/book/a.sy"} {
		if IsAssetDownloadPath(p) {
			t.Fatal(p)
		}
	}
}

func TestAssetDownloadsRecoverApply(t *testing.T) {
	for _, stage := range []string{"prepared", "files-written", "refs-written", "local-edited"} {
		t.Run(stage, func(t *testing.T) {
			base := t.TempDir()
			remote := filepath.Join(base, "cloud")
			full := newAssetTestRepo(t, base, remote, "full", false)
			writeAssetTestFile(t, full, "/assets/test.bin", "asset before", 40)
			writeAssetTestFile(t, full, "/document.txt", "document before", 40)
			syncAssetTestRepo(t, full)
			partial := newAssetTestRepo(t, base, remote, "partial", true)
			syncAssetTestRepo(t, partial)
			beforeDoc := assetTestFile(t, partial, "/document.txt")
			writeAssetTestFile(t, full, "/assets/test.bin", "asset after", 41)
			writeAssetTestFile(t, full, "/document.txt", "document after", 41)
			syncAssetTestRepo(t, full)
			target, err := full.Latest()
			if err != nil {
				t.Fatal(err)
			}
			if _, _, _, err = partial.DownloadIndex(target.ID, nil); err != nil {
				t.Fatal(err)
			}
			afterDoc := assetTestFile(t, full, "/document.txt")
			afterAsset := assetTestFile(t, full, "/assets/test.bin")
			partial.assetDownloads.state.Pending = &assetApply{Index: target, Base: target,
				Deferred: map[string]*entity.File{afterAsset.Path: afterAsset}, Upserts: []*entity.File{afterDoc},
				Before: map[string]*entity.File{beforeDoc.Path: beforeDoc}}
			if err = partial.saveAssetState(); err != nil {
				t.Fatal(err)
			}
			if stage == "files-written" || stage == "refs-written" {
				if err = partial.checkoutFile(afterDoc, partial.DataPath, 1, 1, nil); err != nil {
					t.Fatal(err)
				}
			}
			if stage == "refs-written" {
				if err = partial.UpdateLatest(target); err != nil {
					t.Fatal(err)
				}
			}
			if stage == "local-edited" {
				writeAssetTestFile(t, partial, "/document.txt", "additional local edit", 42)
			}
			if err = partial.ConfigureAssetDownloads(true, partial.assetDownloads.path, "test-scope"); err != nil {
				t.Fatal(err)
			}
			_, _, err = partial.RecoverAssetDownloads(nil)
			if stage == "local-edited" {
				if !errors.Is(err, ErrIndexFileChanged) {
					t.Fatalf("local edit lost during recovery: %v", err)
				}
				if data, readErr := os.ReadFile(partial.absPath("/document.txt")); readErr != nil || string(data) != "additional local edit" {
					t.Fatalf("recovery overwrote edit: %q %v", data, readErr)
				}
				if err = partial.ConfigureAssetDownloads(true, partial.assetDownloads.path, "test-scope"); err != nil {
					t.Fatalf("recovery could not be retried: %v", err)
				}
				syncAssetTestRepo(t, partial)
				syncAssetTestRepo(t, full)
				if data, readErr := os.ReadFile(full.absPath("/document.txt")); readErr != nil || string(data) != "additional local edit" {
					t.Fatalf("retained edit did not sync: %q %v", data, readErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if data, readErr := os.ReadFile(partial.absPath("/document.txt")); readErr != nil || string(data) != "document after" {
				t.Fatalf("recovery failed: %q %v", data, readErr)
			}
			if f := assetTestFile(t, partial, afterAsset.Path); !sameAssetVersion(f, afterAsset) {
				t.Fatalf("logical version was not recovered: %+v", f)
			}
			if deferred, stateErr := partial.DeferredAssets(); stateErr != nil || len(deferred) != 1 || !sameAssetVersion(deferred[0], afterAsset) {
				t.Fatalf("deferred state was not recovered: %+v %v", deferred, stateErr)
			}
			syncAssetTestRepo(t, partial)
		})
	}
}

func TestAssetDownloadsOldInstanceAndMissingChunks(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	full := newAssetTestRepo(t, base, remote, "full", false)
	writeAssetTestFile(t, full, "/assets/test.bin", "old resource", 50)
	syncAssetTestRepo(t, full)
	partial := newAssetTestRepo(t, base, remote, "partial", true)
	syncAssetTestRepo(t, partial)
	stale, err := NewRepo(partial.DataPath, partial.Path, partial.HistoryPath, partial.TempPath, partial.DeviceID,
		partial.DeviceName, partial.DeviceOS, partial.store.AesKey, nil, partial.cloud)
	if err != nil {
		t.Fatal(err)
	}
	if err = stale.ConfigureAssetDownloads(true, partial.assetDownloads.path, "test-scope"); err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, full, "/assets/test.bin", "new resource", 51)
	syncAssetTestRepo(t, full)
	syncAssetTestRepo(t, partial)
	if _, err = stale.EnsureAsset("assets/test.bin", nil); err != nil {
		t.Fatal(err)
	}
	if data, readErr := os.ReadFile(partial.absPath("/assets/test.bin")); readErr != nil || string(data) != "new resource" {
		t.Fatalf("stale instance wrote old version: %q %v", data, readErr)
	}
	if incomplete, checkErr := partial.HasIncompleteSnapshots(); checkErr != nil || !incomplete {
		t.Fatalf("old sparse snapshot was not detected: %v %v", incomplete, checkErr)
	}
	if err = partial.EnsureAllSnapshotChunks(nil); err != nil {
		t.Fatal(err)
	}
	if incomplete, checkErr := partial.HasIncompleteSnapshots(); checkErr != nil || incomplete {
		t.Fatalf("old sparse snapshot was not hydrated: %v %v", incomplete, checkErr)
	}
	writeAssetTestFile(t, full, "/assets/missing.bin", "missing resource", 52)
	syncAssetTestRepo(t, full)
	syncAssetTestRepo(t, partial)
	missing := assetTestFile(t, partial, "/assets/missing.bin")
	_, remoteChunk := full.store.AbsPath(missing.Chunks[0])
	rel, err := filepath.Rel(full.Path, remoteChunk)
	if err != nil {
		t.Fatal(err)
	}
	if err = full.cloud.RemoveObject(filepath.ToSlash(rel)); err != nil {
		t.Fatal(err)
	}
	if _, err = partial.EnsureAsset("assets/missing.bin", nil); err == nil {
		t.Fatal("missing chunk was accepted")
	}
	latest, err := full.Latest()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = partial.DownloadTagIndex("incomplete", latest.ID, nil); err == nil {
		t.Fatal("incomplete snapshot was accepted")
	}
	if _, err = partial.GetTag("incomplete"); err == nil {
		t.Fatal("failed snapshot download created a tag")
	}
	if _, statErr := os.Stat(partial.absPath("/assets/missing.bin")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed download left a resource: %v", statErr)
	}
	deferred, err := partial.DeferredAssets()
	if err != nil || len(deferred) != 1 {
		t.Fatalf("failed download lost logical resource: %v %v", deferred, err)
	}
}

func TestAssetDownloadsClearState(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	full := newAssetTestRepo(t, base, remote, "full", false)
	writeAssetTestFile(t, full, "/assets/test.bin", "resource", 60)
	syncAssetTestRepo(t, full)
	partial := newAssetTestRepo(t, base, remote, "partial", true)
	syncAssetTestRepo(t, partial)
	statePath := partial.assetDownloads.path
	if err := partial.ClearAssetDownloadState(); !errors.Is(err, ErrAssetNotDownloaded) {
		t.Fatalf("incomplete state was cleared: %v", err)
	}
	if err := partial.EnsureAllAssets(nil); err != nil {
		t.Fatal(err)
	}
	if err := partial.EnsureAllSnapshotChunks(nil); err != nil {
		t.Fatal(err)
	}
	if err := partial.ClearAssetDownloadState(); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{statePath, partial.assetStateMarker()} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("state remained: %s %v", p, err)
		}
	}
	if err := partial.ConfigureAssetDownloads(false, statePath, "test-scope"); err != nil {
		t.Fatal(err)
	}
	if partial.assetDownloads != nil {
		t.Fatal("full repository recreated state")
	}
}

func TestAssetDownloadsEmptyFile(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	full := newAssetTestRepo(t, base, remote, "full", false)
	writeAssetTestFile(t, full, "/assets/empty.bin", "", 70)
	syncAssetTestRepo(t, full)
	partial := newAssetTestRepo(t, base, remote, "partial", true)
	syncAssetTestRepo(t, partial)
	file := assetTestFile(t, partial, "/assets/empty.bin")
	if len(file.Chunks) != 1 || file.Size != 0 {
		t.Fatalf("invalid zero-byte resource metadata: %+v", file)
	}
	if downloaded, err := partial.EnsureAsset(file.Path, nil); err != nil || !downloaded {
		t.Fatalf("zero-byte resource could not be downloaded: %v %v", downloaded, err)
	}
	if data, err := os.ReadFile(partial.absPath(file.Path)); err != nil || len(data) != 0 {
		t.Fatalf("zero-byte resource changed: %q %v", data, err)
	}
}

func TestAssetDownloadsSyncIgnore(t *testing.T) {
	for _, scenario := range []string{"local", "cloud", "manual", "already-materialized", "missing-content"} {
		t.Run(scenario, func(t *testing.T) {
			base := t.TempDir()
			remote := filepath.Join(base, "cloud")
			full := newAssetTestRepo(t, base, remote, "full", false)
			writeAssetTestFile(t, full, "/assets/ignored.bin", "ignored resource content", 80)
			syncAssetTestRepo(t, full)
			partial := newAssetTestRepo(t, base, remote, "partial", true)
			syncAssetTestRepo(t, partial)
			file := assetTestFile(t, partial, "/assets/ignored.bin")
			before, err := partial.Latest()
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "missing-content" {
				id := file.Chunks[0]
				if err = full.cloud.RemoveObject("objects/" + id[:2] + "/" + id[2:]); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "local" || scenario == "missing-content" {
				writeAssetTestFile(t, partial, "/.siyuan/syncignore", "/assets/ignored.bin", 81)
				partial.IgnoreLines = []string{"/assets/ignored.bin"}
				_, err = partial.Index("ignore", true, nil)
				if scenario == "missing-content" {
					if err == nil {
						t.Fatal("ignore change discarded an unavailable resource")
					}
					current, latestErr := partial.Latest()
					if latestErr != nil || current.ID != before.ID {
						t.Fatalf("failed ignore change advanced snapshot: %v %v", current, latestErr)
					}
					if deferred, stateErr := partial.DeferredAssets(); stateErr != nil || len(deferred) != 1 {
						t.Fatalf("failed ignore change lost state: %v %v", deferred, stateErr)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				syncAssetTestRepo(t, partial)
			} else {
				writeAssetTestFile(t, full, "/.siyuan/syncignore", "/assets/ignored.bin", 81)
				full.IgnoreLines = []string{"/assets/ignored.bin"}
				syncAssetTestRepo(t, full)
				writeAssetTestFile(t, partial, "/local.txt", "concurrent local document", 82)
				if _, err = partial.Index("local edit", true, nil); err != nil {
					t.Fatal(err)
				}
				if scenario == "already-materialized" {
					if _, err = partial.EnsureAsset(file.Path, nil); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "manual" {
					_, _, err = partial.SyncDownload(nil)
				} else {
					_, _, err = partial.Sync(nil)
				}
				if err != nil {
					t.Fatal(err)
				}
				partial.IgnoreLines = []string{"/assets/ignored.bin"}
			}
			if data, readErr := os.ReadFile(partial.absPath(file.Path)); readErr != nil || string(data) != "ignored resource content" {
				t.Fatalf("ignored resource was not preserved locally: %q %v", data, readErr)
			}
			if deferred, stateErr := partial.DeferredAssets(); stateErr != nil || len(deferred) != 0 {
				t.Fatalf("ignored resource remains deferred: %v %v", deferred, stateErr)
			}
			syncAssetTestRepo(t, partial)
			index, latestErr := partial.Latest()
			if latestErr != nil {
				t.Fatal(latestErr)
			}
			files, filesErr := partial.GetFiles(index)
			if filesErr != nil {
				t.Fatal(filesErr)
			}
			for _, f := range files {
				if f.Path == file.Path {
					t.Fatal("ignored resource was reintroduced into logical index")
				}
			}
			cloudIndex, cloudErr := partial.GetCloudLatest(nil)
			if cloudErr != nil {
				t.Fatal(cloudErr)
			}
			for _, id := range cloudIndex.Files {
				if id == file.ID {
					t.Fatal("ignored resource was reuploaded")
				}
			}
		})
	}
}
