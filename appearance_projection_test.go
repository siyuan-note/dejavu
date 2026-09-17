package dejavu

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/restic/chunker"
	"github.com/siyuan-note/dejavu/entity"
)

func newAppearanceProjectionTestRepo(t *testing.T) *Repo {
	t.Helper()
	root := t.TempDir()
	repo, err := NewRepoWithOptions(Options{DataPath: filepath.Join(root, "data"), RepoPath: filepath.Join(root, "repo"),
		HistoryPath: filepath.Join(root, "history"), TempPath: filepath.Join(root, "temp"),
		DeviceID: "projection", AESKey: []byte("0123456789abcdef0123456789abcdef"), EnableAppearanceSync: true})
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

func projectionTestArchive(t *testing.T, key, content string, parents ...string) *appearanceArchive {
	t.Helper()
	files := map[string][]byte{"theme.css": []byte(content), "assets/font.bin": []byte("complete package font")}
	if content == "" {
		files = map[string][]byte{}
	}
	return &appearanceArchive{Key: key, Parents: parents, Files: files, State: appearanceArchiveTestState(t, files, content == "")}
}

func putProjectionTestArchive(t *testing.T, repo *Repo, archive *appearanceArchive) *entity.File {
	t.Helper()
	data, err := encodeAppearanceArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	file, err := repo.putAppearanceBytes(appearanceArchivePath(archive), data, true)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func writeProjectionTestFiles(t *testing.T, repo *Repo, archive *appearanceArchive) {
	t.Helper()
	writeAssetTestFile(t, repo, appearanceRecordPath(archive.Key), string(archive.State), 1)
	for relative, data := range archive.Files {
		writeAssetTestFile(t, repo, archive.Key+"/"+relative, string(data), 1)
	}
}

func TestPutAppearanceBytesMatchesStandardChunks(t *testing.T) {
	for _, size := range []int{0, chunker.MinSize - 1, chunker.MinSize, 2*chunker.MaxSize + 31} {
		t.Run(string(rune(size%26+'a'))+"/"+time.Duration(size).String(), func(t *testing.T) {
			repo := newAppearanceProjectionTestRepo(t)
			data := make([]byte, size)
			if _, err := rand.New(rand.NewSource(1)).Read(data); err != nil {
				t.Fatal(err)
			}
			virtual, err := repo.putAppearanceBytes("/themes/custom/resource.bin", data, false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = repo.store.Stat(virtual.ID); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("virtual file metadata entered object store: %v", err)
			}
			opened, err := repo.openFile(virtual)
			if err != nil || !bytes.Equal(opened, data) {
				t.Fatalf("virtual chunks did not restore bytes: %v", err)
			}
			standardPath := "/assets/standard.bin"
			writeAssetTestFile(t, repo, standardPath, string(data), 1)
			stamp := time.UnixMilli(appearanceEventModified)
			if err = os.Chtimes(repo.absPath(standardPath), stamp, stamp); err != nil {
				t.Fatal(err)
			}
			standard := entity.NewFile(standardPath, int64(size), appearanceEventModified)
			if err = repo.putFileChunks(standard, nil, 1, 1); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(virtual.Chunks, standard.Chunks) {
				t.Fatal("projection chunk boundaries differ from old engine")
			}
			p := "/storage/appearance-v1/themes/custom/" + appearanceArchiveDigest(data) + ".sypkg"
			persistent, err := repo.putAppearanceBytes(p, data, true)
			if err != nil {
				t.Fatal(err)
			}
			stored, err := repo.store.GetFile(persistent.ID)
			if err != nil || !reflect.DeepEqual(stored, persistent) || !reflect.DeepEqual(persistent.Chunks, standard.Chunks) ||
				persistent.ID != entity.NewFile(p, int64(size), appearanceEventModified).ID {
				t.Fatalf("persistent archive departed from legacy identity or chunks: %v", err)
			}
			if _, err = repo.putAppearanceBytes(p, data, true); err != nil {
				t.Fatalf("immutable object was not idempotent: %v", err)
			}
		})
	}
}

func TestPutAppearanceBytesPersistsCachedMetadata(t *testing.T) {
	first := newAppearanceProjectionTestRepo(t)
	archive := projectionTestArchive(t, "/themes/cached", "same archive across stores")
	data, err := encodeAppearanceArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	p := appearanceArchivePath(archive)
	file, err := first.putAppearanceBytes(p, data, true)
	if err != nil {
		t.Fatal(err)
	}
	fileCache.Wait()
	second := newAppearanceProjectionTestRepo(t)
	if _, err = second.store.Stat(file.ID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fresh store unexpectedly has metadata: %v", err)
	}
	if _, err = second.putAppearanceBytes(p, data, true); err != nil {
		t.Fatal(err)
	}
	if _, err = second.store.Stat(file.ID); err != nil {
		t.Fatalf("cached metadata was not persisted in second store: %v", err)
	}
	fileCache.Wait()
	_, objectPath := second.store.AbsPath(file.ID)
	if err = os.Remove(objectPath); err != nil {
		t.Fatal(err)
	}
	if _, err = second.putAppearanceBytes(p, data, true); err != nil {
		t.Fatal(err)
	}
	if _, err = second.store.Stat(file.ID); err != nil {
		t.Fatalf("cached metadata was not restored after object removal: %v", err)
	}
}

func TestAppearanceProjectionFilesSelectsWholePackage(t *testing.T) {
	repo := newAppearanceProjectionTestRepo(t)
	base := projectionTestArchive(t, "/themes/custom", "base")
	baseFile := putProjectionTestArchive(t, repo, base)
	latest := projectionTestArchive(t, base.Key, "latest", base.Digest)
	latestFile := putProjectionTestArchive(t, repo, latest)
	legacy := entity.NewFile("/icons/legacy/icon.js", 1, 1)
	shadow := entity.NewFile("/themes/custom/obsolete.css", 1, 1)
	files, err := repo.appearanceProjectionFiles([]*entity.File{latestFile, legacy, shadow, baseFile}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != len(latest.Files)+2 || files[legacy.Path] != legacy || files[shadow.Path] != nil {
		t.Fatal("event projection lost legacy files or retained mixed package content")
	}
	for p, file := range files {
		if file == legacy {
			continue
		}
		if _, err = repo.store.Stat(file.ID); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("projection file entered snapshot object store: %s", p)
		}
		content, openErr := repo.openFile(file)
		if openErr != nil {
			t.Fatal(openErr)
		}
		want := latest.State
		if p != appearanceRecordPath(latest.Key) {
			want = latest.Files[strings.TrimPrefix(p, latest.Key+"/")]
		}
		if !bytes.Equal(content, want) {
			t.Fatalf("projection mixed causal versions: %s", p)
		}
	}
	deleted := projectionTestArchive(t, base.Key, "", latest.Digest)
	deletedFile := putProjectionTestArchive(t, repo, deleted)
	files, err = repo.appearanceProjectionFiles([]*entity.File{baseFile, latestFile, deletedFile}, nil)
	if err != nil || len(files) != 1 || files[appearanceRecordPath(base.Key)] == nil {
		t.Fatalf("tombstone materialized payload: %v", err)
	}
	if _, err = repo.appearanceProjectionFiles([]*entity.File{latestFile}, nil); err == nil {
		t.Fatal("missing causal parent accepted")
	}
}

func TestAppearanceProjectionIgnoredFilesDoNotFetch(t *testing.T) {
	repo := newAppearanceProjectionTestRepo(t)
	repo.appearanceIgnoreLines = []string{"/themes/private/"}
	missing := entity.NewFile("/storage/appearance-v1/themes/private/"+strings.Repeat("a", 64)+".sypkg", 1, appearanceEventModified)
	legacy := entity.NewFile("/themes/private/theme.css", 1, 1)
	files, err := repo.appearanceProjectionFiles([]*entity.File{missing, legacy}, nil)
	if err != nil || len(files) != 0 {
		t.Fatalf("ignored package was fetched or projected: %v", err)
	}
}

func TestAppearanceProjectionBeforeUsesOnlyMatchingHeads(t *testing.T) {
	repo := newAppearanceProjectionTestRepo(t)
	base := projectionTestArchive(t, "/themes/custom", "ancestor")
	baseFile := putProjectionTestArchive(t, repo, base)
	deleted := projectionTestArchive(t, base.Key, "", base.Digest)
	deletedFile := putProjectionTestArchive(t, repo, deleted)
	migration := projectionTestArchive(t, base.Key, "local migration")
	migrationFile := putProjectionTestArchive(t, repo, migration)
	input := []*entity.File{baseFile, deletedFile, migrationFile}
	writeProjectionTestFiles(t, repo, migration)
	canonical, err := repo.appearanceProjectionFiles(input, nil)
	if err != nil || len(canonical) != 1 {
		t.Fatalf("canonical projection did not choose deletion: %v", err)
	}
	before, err := repo.appearanceBeforeProjectionFiles(input, nil)
	if err != nil || len(before) != len(migration.Files)+1 {
		t.Fatalf("known matching migration head was not used as before: %v", err)
	}
	content, err := repo.openFile(before[migration.Key+"/theme.css"])
	if err != nil || string(content) != "local migration" {
		t.Fatalf("before version mixed heads: %v", err)
	}
	writeProjectionTestFiles(t, repo, base)
	before, err = repo.appearanceBeforeProjectionFiles(input, nil)
	if err != nil || !reflect.DeepEqual(before, canonical) {
		t.Fatalf("ancestor rollback was blessed as before: %v", err)
	}
	writeProjectionTestFiles(t, repo, migration)
	writeAssetTestFile(t, repo, migration.Key+"/theme.css", "unindexed external edit", 1)
	before, err = repo.appearanceBeforeProjectionFiles(input, nil)
	if err != nil || !reflect.DeepEqual(before, canonical) {
		t.Fatalf("arbitrary external edit was blessed as before: %v", err)
	}
}

func TestAppearanceProjectionDoesNotOverwriteLegacyFileObject(t *testing.T) {
	repo := newAppearanceProjectionTestRepo(t)
	archive := projectionTestArchive(t, "/themes/custom", "new archive content")
	legacy, err := repo.putAppearanceBytes(archive.Key+"/theme.css", []byte("old legacy content"), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.store.PutFile(legacy); err != nil {
		t.Fatal(err)
	}
	projection, err := repo.appearanceArchiveProjectionFiles(archive)
	if err != nil {
		t.Fatal(err)
	}
	virtual := projection[legacy.Path]
	if virtual.ID != legacy.ID || reflect.DeepEqual(virtual.Chunks, legacy.Chunks) {
		t.Fatal("fixture did not exercise two journal versions sharing legacy metadata ID")
	}
	stored, err := repo.store.GetFile(legacy.ID)
	if err != nil || !reflect.DeepEqual(stored, legacy) {
		t.Fatalf("projection changed historical legacy object: %v", err)
	}
	oldContent, err := repo.openFile(stored)
	if err != nil || string(oldContent) != "old legacy content" {
		t.Fatalf("legacy snapshot lost original bytes: %v", err)
	}
	newContent, err := repo.openFile(virtual)
	if err != nil || string(newContent) != "new archive content" {
		t.Fatalf("journal version did not preserve independent bytes: %v", err)
	}
}

type projectionTestDiskEntry struct {
	Data     string
	Mode     fs.FileMode
	Modified time.Time
}

func snapshotProjectionTestDisk(t *testing.T, root string) map[string]projectionTestDiskEntry {
	t.Helper()
	ret := map[string]projectionTestDiskEntry{}
	err := filepath.WalkDir(root, func(p string, entry fs.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) && p == root {
			return nil
		}
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		value := projectionTestDiskEntry{Mode: info.Mode()}
		if info.Mode().IsRegular() {
			value.Modified = info.ModTime()
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			value.Data = string(data)
		}
		ret[p] = value
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return ret
}

func assertProjectionChangedReadOnly(t *testing.T, repo *Repo, changed, failed bool) {
	t.Helper()
	root := filepath.Dir(strings.TrimSuffix(repo.DataPath, string(os.PathSeparator)))
	before := snapshotProjectionTestDisk(t, root)
	got, err := repo.appearanceProjectionChanged()
	if got != changed || (err != nil) != failed {
		t.Fatalf("changed=%v err=%v, want changed=%v failed=%v", got, err, changed, failed)
	}
	after := snapshotProjectionTestDisk(t, root)
	if !reflect.DeepEqual(before, after) {
		for p, previous := range before {
			if !reflect.DeepEqual(previous, after[p]) {
				t.Logf("changed %s: before mode=%v time=%v bytes=%d, after mode=%v time=%v bytes=%d", p,
					previous.Mode, previous.Modified, len(previous.Data), after[p].Mode, after[p].Modified, len(after[p].Data))
			}
		}
		for p := range after {
			if _, exists := before[p]; !exists {
				t.Logf("created %s", p)
			}
		}
		t.Fatal("projection check mutated data, state, or object store")
	}
}

func TestAppearanceProjectionChangedIsReadOnly(t *testing.T) {
	repo := newAppearanceProjectionTestRepo(t)
	archive := projectionTestArchive(t, "/themes/custom", "original")
	archive.Parents = nil
	if _, err := repo.writeAppearanceArchive(archive); err != nil {
		t.Fatal(err)
	}
	writeProjectionTestFiles(t, repo, archive)
	assertProjectionChangedReadOnly(t, repo, false, false)
	writeAssetTestFile(t, repo, archive.Key+"/theme.css", "manual CSS", 1)
	assertProjectionChangedReadOnly(t, repo, true, false)
	writeProjectionTestFiles(t, repo, archive)
	writeAssetTestFile(t, repo, archive.Key+"/theme.js", "manual new JS", 1)
	assertProjectionChangedReadOnly(t, repo, true, false)
	if err := os.Remove(repo.absPath(archive.Key + "/theme.js")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(repo.absPath(archive.Key + "/assets/font.bin")); err != nil {
		t.Fatal(err)
	}
	assertProjectionChangedReadOnly(t, repo, true, false)
	writeProjectionTestFiles(t, repo, archive)
	var state map[string]any
	if err := json.Unmarshal(archive.State, &state); err != nil {
		t.Fatal(err)
	}
	state["version"] = 99
	data, _ := json.Marshal(state)
	writeAssetTestFile(t, repo, appearanceRecordPath(archive.Key), string(data), 1)
	assertProjectionChangedReadOnly(t, repo, false, true)
	writeProjectionTestFiles(t, repo, archive)
	statePath := repo.absPath(appearanceRecordPath(archive.Key))
	if err := os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(statePath, 0755); err != nil {
		t.Fatal(err)
	}
	assertProjectionChangedReadOnly(t, repo, false, true)
}

func TestAppearanceProjectionChangedNewDeletedAndIgnored(t *testing.T) {
	repo := newAppearanceProjectionTestRepo(t)
	assertProjectionChangedReadOnly(t, repo, false, false)
	archive := projectionTestArchive(t, "/themes/custom", "new package")
	writeProjectionTestFiles(t, repo, archive)
	assertProjectionChangedReadOnly(t, repo, true, false)
	if _, err := repo.writeAppearanceArchive(archive); err != nil {
		t.Fatal(err)
	}
	assertProjectionChangedReadOnly(t, repo, false, false)
	deleted := projectionTestArchive(t, archive.Key, "", archive.Digest)
	if _, err := repo.writeAppearanceArchive(deleted); err != nil {
		t.Fatal(err)
	}
	writeProjectionTestFiles(t, repo, deleted)
	if err := os.RemoveAll(repo.absPath(archive.Key)); err != nil {
		t.Fatal(err)
	}
	assertProjectionChangedReadOnly(t, repo, false, false)
	writeAssetTestFile(t, repo, appearanceRecordPath(archive.Key), "invalid ignored local state", 1)
	for _, rule := range []string{"/themes/custom/", "/storage/appearance-v1/themes/custom/", "/storage/", "/storage/appearance-v1/"} {
		repo.appearanceIgnoreLines = []string{rule}
		assertProjectionChangedReadOnly(t, repo, false, false)
	}
	repo.appearanceIgnoreLines = []string{"/themes/custom/theme.css"}
	writeProjectionTestFiles(t, repo, archive)
	assertProjectionChangedReadOnly(t, repo, false, true)
}

func TestAppearanceProjectionDirectoryFiltering(t *testing.T) {
	repo := newAppearanceProjectionTestRepo(t)
	files := map[string][]byte{
		"theme.css": []byte("css"), ".siyuan/settings.json": []byte("settings"),
		"assets/.siyuan/settings.json": []byte("nested settings"), "assets.tmp/font.woff": []byte("font"),
	}
	archive := &appearanceArchive{Key: "/themes/custom", Files: files, State: appearanceArchiveTestState(t, files, false)}
	writeProjectionTestFiles(t, repo, archive)
	for _, name := range []string{".git/config", ".siyuan/.draft", ".siyuan/cache.tmp", "assets/.private/config"} {
		writeAssetTestFile(t, repo, archive.Key+"/"+name, "local ignored file", 1)
	}
	if _, err := repo.writeAppearanceArchive(archive); err != nil {
		t.Fatal(err)
	}
	projection, err := repo.readAppearanceProjection(archive.Key)
	if err != nil || !sameAppearanceProjection(archive, projection) {
		t.Fatalf("package scan changed the supported file set: %+v %v", projection, err)
	}
	assertProjectionChangedReadOnly(t, repo, false, false)
	writeAssetTestFile(t, repo, archive.Key+"/.siyuan/settings.json", "edited settings", 1)
	assertProjectionChangedReadOnly(t, repo, true, false)
	if _, err = repo.readAppearanceProjection(archive.Key); err == nil {
		t.Fatal("unrecorded settings modification was accepted")
	}
}
