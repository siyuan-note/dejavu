package dejavu

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/siyuan-note/dejavu/entity"
)

func TestAppearanceArchiveCacheRejectsRecentOrFutureChangeTime(t *testing.T) {
	if appearanceCacheChangeTimeStable(time.Time{}) || appearanceCacheChangeTimeStable(time.Now()) ||
		appearanceCacheChangeTimeStable(time.Now().Add(time.Hour)) {
		t.Fatal("unstable change time enabled metadata reuse")
	}
	if !appearanceCacheChangeTimeStable(time.Now().Add(-2 * time.Second)) {
		t.Fatal("stable change time disabled metadata reuse")
	}
}

func TestAppearanceArchiveCacheChecksSourceChanges(t *testing.T) {
	repo, _ := newAppearanceRefRepo(t)
	archive := appearanceRefEvent(t, false)
	if _, err := repo.writeAppearanceArchive(archive); err != nil {
		t.Fatal(err)
	}
	abs := repo.absPath(appearanceArchivePath(archive))
	reads := 0
	read := func() ([]byte, error) {
		reads++
		return os.ReadFile(abs)
	}
	load := func(full bool) (*appearanceArchive, error) {
		return cachedAppearanceArchive("cache-test", archive.Key, archive.Digest, []string{abs}, full, read)
	}
	metadata, err := load(false)
	if err != nil || metadata.Files != nil || !bytes.Equal(metadata.State, archive.State) {
		t.Fatalf("invalid cached metadata: %+v %v", metadata, err)
	}
	full, err := load(true)
	if err != nil || !sameAppearanceProjection(full, archive) {
		t.Fatalf("cached payload differs: %v", err)
	}
	stamp, err := appearanceSourceStamp([]string{abs})
	if err != nil {
		t.Fatal(err)
	}
	if stamp != "" && reads != 1 {
		t.Fatalf("unchanged authenticated source was read %d times", reads)
	}
	info, err := os.Stat(abs)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 1
	if err = os.WriteFile(abs, data, info.Mode().Perm()); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(abs, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if _, err = load(true); err == nil {
		t.Fatal("preserved timestamp concealed changed archive content")
	}
	preserved, err := os.ReadFile(abs)
	if err != nil || !bytes.Equal(preserved, data) {
		t.Fatalf("failed validation changed source bytes: %v", err)
	}
}

func TestAppearanceArchiveCacheAuthenticatesStoreAndKey(t *testing.T) {
	for _, change := range []string{"key", "ciphertext"} {
		t.Run(change, func(t *testing.T) {
			repo, _ := newAppearanceRefRepo(t)
			index := appearanceRefIndex(t, repo, "note", appearanceFormatData, appearanceRefEvent(t, false))
			files, err := repo.getFiles(index.Files)
			if err != nil {
				t.Fatal(err)
			}
			var event *entity.File
			for _, file := range files {
				if key, _ := appearanceArchiveKey(file.Path); key != "" {
					event = file
				}
			}
			if _, err = repo.readAppearanceStoredArchive(event, nil, true); err != nil {
				t.Fatal(err)
			}
			if change == "key" {
				previous := repo.store.AesKey
				repo.store.AesKey = append([]byte(nil), previous...)
				repo.store.AesKey[0] ^= 1
				defer func() { repo.store.AesKey = previous }()
			} else {
				_, abs := repo.store.AbsPath(event.Chunks[0])
				info, statErr := os.Stat(abs)
				if statErr != nil {
					t.Fatal(statErr)
				}
				data, readErr := os.ReadFile(abs)
				if readErr != nil {
					t.Fatal(readErr)
				}
				data[len(data)/2] ^= 1
				if err = os.WriteFile(abs, data, info.Mode().Perm()); err != nil {
					t.Fatal(err)
				}
				if err = os.Chtimes(abs, info.ModTime(), info.ModTime()); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = repo.readAppearanceStoredArchive(event, nil, true); err == nil {
				t.Fatal("cached payload bypassed source authentication")
			}
		})
	}
}

func TestAppearanceArchiveCacheBoundsAndIsolatesMetadata(t *testing.T) {
	cache := newAppearanceArchiveCache(1024)
	archive := &appearanceArchive{Key: "/themes/example", State: []byte("original"), Parents: []string{"parent"}}
	cache.put("first", archive, false)
	copy := cache.get("first")
	copy.State[0] = 'x'
	copy.Parents[0] = "changed"
	if got := cache.get("first"); string(got.State) != "original" || got.Parents[0] != "parent" {
		t.Fatal("caller changed cached metadata")
	}
	for _, key := range []string{"second", "third", "fourth", "fifth"} {
		cache.put(key, archive, false)
	}
	if cache.size > cache.limit || len(cache.entries) >= 5 || cache.get("first") != nil {
		t.Fatalf("unbounded cache: bytes=%d entries=%d", cache.size, len(cache.entries))
	}
}
