package dejavu

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

func TestAppearanceIsolationCallbackRecovery(t *testing.T) {
	for _, externalEdit := range []bool{false, true} {
		t.Run(map[bool]string{false: "resume", true: "preserve-user-edit"}[externalEdit], func(t *testing.T) {
			repo, pending, key := newAppearancePendingTest(t)
			raw := []byte("/assets/private/\n")
			file := entity.NewFile(repo.ignoreRulePath, int64(len(raw)), 1700000010000)
			file.Chunks = []string{util.Hash(raw)}
			if err := repo.store.PutChunk(&entity.Chunk{ID: file.Chunks[0], Data: raw}); err != nil {
				t.Fatal(err)
			}
			pending.Upserts = append(pending.Upserts, file)
			pending.Before[file.Path] = nil
			if err := repo.saveAssetState(); err != nil {
				t.Fatal(err)
			}
			canonical := string(raw) + strings.Join([]string{"# siyuan-appearance-isolation:v1:begin",
				"/themes/", "/icons/", "/storage/bazaar/themes/", "/storage/bazaar/icons/",
				"# siyuan-appearance-isolation:v1:end", ""}, "\n")
			interrupted := errors.New("interrupted after isolation")
			first := true
			repo.beforeAppearanceApply = func() error {
				data, err := os.ReadFile(repo.absPath(repo.ignoreRulePath))
				if err != nil {
					return err
				}
				if string(data) != canonical {
					if err = os.WriteFile(repo.absPath(repo.ignoreRulePath), []byte(canonical), 0644); err != nil {
						return err
					}
					tick := time.UnixMilli(file.Updated + 100000)
					if err = os.Chtimes(repo.absPath(repo.ignoreRulePath), tick, tick); err != nil {
						return err
					}
				}
				if first {
					first = false
					return interrupted
				}
				return nil
			}
			if err := repo.recoverAssetApply(nil, false); !errors.Is(err, interrupted) {
				t.Fatalf("callback interruption not retained: %v", err)
			}
			if externalEdit {
				if err := os.WriteFile(repo.absPath(repo.ignoreRulePath), []byte(canonical+"/assets/new-private/\n"), 0644); err != nil {
					t.Fatal(err)
				}
				if err := repo.recoverAssetApply(nil, false); !errors.Is(err, ErrIndexFileChanged) {
					t.Fatalf("user ignore edit was overwritten: %v", err)
				}
				if got := readAppearanceTestFile(t, repo, key+"/theme.css"); got != "old css" {
					t.Fatalf("package published before resolving user edit: %s", got)
				}
				return
			}
			if err := repo.recoverAssetApply(nil, false); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(repo.absPath(repo.ignoreRulePath))
			if err != nil || info.ModTime().UnixMilli() != file.Updated+100000 {
				t.Fatalf("replay realigned managed content to old identity: %v %v", info, err)
			}
			if got := readAppearanceTestFile(t, repo, key+"/theme.css"); got != "new css" {
				t.Fatalf("package did not publish on recovery: %s", got)
			}
		})
	}
}
