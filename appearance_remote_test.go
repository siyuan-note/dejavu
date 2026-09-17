package dejavu

import (
	"encoding/json"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

func uploadAppearanceRemoteFixture(t *testing.T, repo *Repo, index *entity.Index) []*entity.File {
	t.Helper()
	files, err := repo.getFiles(index.Files)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		for _, id := range append([]string{file.ID}, file.Chunks...) {
			if _, err = repo.cloud.UploadObject(path.Join("objects", id[:2], id[2:]), false); err != nil {
				t.Fatal(err)
			}
		}
	}
	return files
}

func TestAppearanceRemoteFormatPreservesLegacyAndAcceptsCurrent(t *testing.T) {
	repo, _ := newAppearanceRefRepo(t)
	legacy := appearanceRefIndex(t, repo, "legacy note", "")
	files := uploadAppearanceRemoteFixture(t, repo, legacy)
	stat, err := repo.validateRemoteAppearanceFormat(legacy, files, nil)
	if err != nil || stat.APIGet != 0 {
		t.Fatalf("legacy snapshot required appearance data: %+v %v", stat, err)
	}
	current := appearanceRefIndex(t, repo, "current note", appearanceFormatData, appearanceRefEvent(t, false))
	files = uploadAppearanceRemoteFixture(t, repo, current)
	stat, err = repo.validateRemoteAppearanceFormat(current, files, nil)
	if err != nil || stat.APIGet < 1 {
		t.Fatalf("current format did not authenticate its cloud marker: %+v %v", stat, err)
	}
}

func TestAppearanceRemoteFormatRejectsMalformedBoundaries(t *testing.T) {
	for _, name := range []string{"missing-marker", "different-marker-id", "unknown-file", "duplicate-path", "case-collision", "case-alias", "ignored-marker"} {
		t.Run(name, func(t *testing.T) {
			repo, _ := newAppearanceRefRepo(t)
			index := appearanceRefIndex(t, repo, "unchanged note", appearanceFormatData, appearanceRefEvent(t, false))
			files := uploadAppearanceRemoteFixture(t, repo, index)
			var marker, event *entity.File
			for _, file := range files {
				if file.Path == appearanceFormatPath {
					marker = file
				} else if key, _ := appearanceArchiveKey(file.Path); key != "" {
					event = file
				}
			}
			switch name {
			case "missing-marker":
				var kept []*entity.File
				index.Files = nil
				for _, file := range files {
					if file != marker {
						kept = append(kept, file)
						index.Files = append(index.Files, file.ID)
					}
				}
				files = kept
			case "different-marker-id":
				copy := *marker
				copy.Updated += 1000
				copy.ID = entity.NewFile(copy.Path, copy.Size, copy.Updated).ID
				for i, file := range files {
					if file == marker {
						files[i] = &copy
					}
				}
				index.Files = append(index.Files, copy.ID)
			case "unknown-file":
				file := entity.NewFile("/storage/appearance-v1/unknown.json", 1, appearanceEventModified)
				file.Chunks = []string{util.Hash([]byte("x"))}
				files = append(files, file)
				index.Files = append(index.Files, file.ID)
			case "duplicate-path":
				files = append(files, event)
			case "case-collision", "case-alias":
				copy := *event
				copy.Path = strings.Replace(copy.Path, "/ref-test/", "/REF-TEST/", 1)
				if name == "case-alias" {
					copy.Path = strings.Replace(copy.Path, "/storage/", "/Storage/", 1)
				}
				copy.ID = entity.NewFile(copy.Path, copy.Size, copy.Updated).ID
				files = append(files, &copy)
				index.Files = append(index.Files, copy.ID)
			case "ignored-marker":
				repo.appearanceIgnoreLines = []string{appearanceFormatPath}
			}
			if _, err := repo.validateRemoteAppearanceFormat(index, files, nil); err == nil {
				t.Fatal("invalid protocol boundary was accepted")
			}
		})
	}
}

func TestAppearanceRemoteFormatRejectsSameIDCloudMutation(t *testing.T) {
	repo, _ := newAppearanceRefRepo(t)
	index := appearanceRefIndex(t, repo, "unchanged note", appearanceFormatData)
	files := uploadAppearanceRemoteFixture(t, repo, index)
	marker := entity.NewFile(appearanceFormatPath, int64(len(appearanceFormatData)), appearanceEventModified)
	_, markerPath := repo.store.AbsPath(marker.ID)
	before, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	unknown := []byte(strings.Replace(appearanceFormatData, `"version":1`, `"version":2`, 1))
	marker.Chunks = []string{util.Hash(unknown)}
	metadata, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	for id, data := range map[string][]byte{marker.ID: metadata, marker.Chunks[0]: unknown} {
		encoded, encodeErr := repo.store.encodeData(data)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		if _, err = repo.cloud.UploadBytes(path.Join("objects", id[:2], id[2:]), encoded, true); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = repo.validateRemoteAppearanceFormat(index, files, nil); err == nil {
		t.Fatal("valid local marker concealed a different cloud marker with the same legacy ID")
	}
	after, err := os.ReadFile(markerPath)
	if err != nil || string(before) != string(after) {
		t.Fatalf("rejected cloud marker replaced the local source: %v", err)
	}
}

func TestAppearanceRemoteFormatHonorsWholeIgnoreAndDisabledMode(t *testing.T) {
	for _, scope := range []string{"disabled", "/storage/", "/storage/appearance-v1/", "/themes/ref-test/"} {
		t.Run(scope, func(t *testing.T) {
			repo, _ := newAppearanceRefRepo(t)
			repo.appearanceSyncEnabled = scope != "disabled"
			repo.appearanceIgnoreLines = []string{scope}
			file := entity.NewFile("/storage/appearance-v1/themes/ref-test/unknown.json", 1, appearanceEventModified)
			file.Chunks = []string{util.Hash([]byte("x"))}
			index := &entity.Index{Files: []string{file.ID}}
			stat, err := repo.validateRemoteAppearanceFormat(index, []*entity.File{file}, nil)
			if err != nil || stat.APIGet != 0 {
				t.Fatalf("ignored appearance data was inspected: %+v %v", stat, err)
			}
		})
	}
}
