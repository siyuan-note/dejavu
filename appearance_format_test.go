package dejavu

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

func TestAppearanceLocalFormatRejectsUnknownSnapshots(t *testing.T) {
	for _, name := range []string{"missing-marker", "different-marker-id", "same-id-mutation", "unknown-file"} {
		t.Run(name, func(t *testing.T) {
			repo, _ := newAppearanceRefRepo(t)
			index := appearanceRefIndex(t, repo, "unchanged", appearanceFormatData, appearanceRefEvent(t, false))
			if ok, err := repo.isAppearanceIndex(index, nil); err != nil || !ok {
				t.Fatalf("valid local format: %v %v", ok, err)
			}
			marker := entity.NewFile(appearanceFormatPath, int64(len(appearanceFormatData)), appearanceEventModified)
			switch name {
			case "missing-marker":
				var kept []string
				for _, id := range index.Files {
					if id != marker.ID {
						kept = append(kept, id)
					}
				}
				index.Files = kept
			case "different-marker-id":
				unknown := *marker
				unknown.Updated += 1000
				unknown.ID = entity.NewFile(unknown.Path, unknown.Size, unknown.Updated).ID
				unknown.Chunks = []string{util.Hash([]byte(appearanceFormatData))}
				if err := repo.store.PutFile(&unknown); err != nil {
					t.Fatal(err)
				}
				for i, id := range index.Files {
					if id == marker.ID {
						index.Files[i] = unknown.ID
					}
				}
			case "same-id-mutation":
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
					dir, abs := repo.store.AbsPath(id)
					if err = os.MkdirAll(dir, 0755); err != nil {
						t.Fatal(err)
					}
					if err = os.WriteFile(abs, encoded, 0644); err != nil {
						t.Fatal(err)
					}
				}
			case "unknown-file":
				unknown := entity.NewFile("/storage/appearance-v1/unknown.json", 1, appearanceEventModified)
				unknown.Chunks = []string{util.Hash([]byte("x"))}
				if err := repo.store.PutFile(unknown); err != nil {
					t.Fatal(err)
				}
				index.Files = append(index.Files, unknown.ID)
			}
			if ok, err := repo.isAppearanceIndex(index, nil); err == nil || ok {
				t.Fatalf("unknown format treated as an old snapshot: %v %v", ok, err)
			}
		})
	}
}
