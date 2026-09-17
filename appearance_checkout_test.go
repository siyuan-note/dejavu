package dejavu

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

func appearanceCheckoutRule(t *testing.T, repo *Repo, index *entity.Index, contents string, tick int64) {
	t.Helper()
	data := []byte(contents)
	file := entity.NewFile(repo.ignoreRulePath, int64(len(data)), time.Unix(1700000000+tick, 0).UnixMilli())
	file.Chunks = []string{util.Hash(data)}
	if err := repo.store.PutChunk(&entity.Chunk{ID: file.Chunks[0], Data: data}); err != nil {
		t.Fatal(err)
	}
	if err := repo.store.PutFile(file); err != nil {
		t.Fatal(err)
	}
	index.Files = append(index.Files, file.ID)
	index.Count++
	index.Size += file.Size
	if err := repo.store.PutIndex(index); err != nil {
		t.Fatal(err)
	}
}

func TestAppearanceCheckoutUsesTargetIgnoreRules(t *testing.T) {
	managed := "# siyuan-appearance-isolation:v1:begin\n/themes/\n/icons/\n" +
		"/storage/bazaar/themes/\n/storage/bazaar/icons/\n# siyuan-appearance-isolation:v1:end\n"
	for i, test := range []struct {
		name          string
		currentIgnore []string
		targetRules   string
		want          string
		fail          bool
	}{
		{name: "target excludes package", targetRules: "/themes/custom/\n" + managed, want: "current"},
		{name: "target enables package", currentIgnore: []string{"/themes/custom/"}, targetRules: managed, want: "historical"},
		{name: "target without rule file enables package", currentIgnore: []string{"/themes/custom/"}, want: "historical"},
		{name: "unknown target isolation version", targetRules: strings.ReplaceAll(managed, ":v1:", ":v2:"), want: "current", fail: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			repo, err := NewRepoWithOptions(Options{DataPath: filepath.Join(root, "data"), RepoPath: filepath.Join(root, "repo"),
				HistoryPath: filepath.Join(root, "history"), TempPath: filepath.Join(root, "temp"),
				DeviceID: "checkout", AESKey: []byte("0123456789abcdef0123456789abcdef"), EnableAppearanceSync: true,
				IgnoreRulePath: ".siyuan/syncignore", AppearanceIgnoreLines: test.currentIgnore})
			if err != nil {
				t.Fatal(err)
			}
			old := projectionTestArchive(t, "/themes/custom", "historical")
			target := appearanceRefIndex(t, repo, "target notebook", appearanceFormatData, old)
			if test.targetRules != "" {
				appearanceCheckoutRule(t, repo, target, test.targetRules, int64(40000+i*10))
			}
			current := projectionTestArchive(t, old.Key, "current", old.Digest)
			latest := appearanceRefIndex(t, repo, "current notebook", appearanceFormatData, old, current)
			currentRules := strings.Join(test.currentIgnore, "\n") + "\n# current rules\n"
			appearanceCheckoutRule(t, repo, latest, currentRules, int64(40001+i*10))
			files, err := repo.getFiles(latest.Files)
			if err != nil {
				t.Fatal(err)
			}
			if err = repo.checkoutFiles(files, nil); err != nil {
				t.Fatal(err)
			}
			writeProjectionTestFiles(t, repo, current)
			if err = repo.UpdateLatest(latest); err != nil {
				t.Fatal(err)
			}
			_, _, err = repo.Checkout(target.ID, nil)
			if (err != nil) != test.fail {
				t.Fatalf("checkout error: %v", err)
			}
			content, readErr := os.ReadFile(repo.absPath(old.Key + "/theme.css"))
			if readErr != nil || string(content) != test.want {
				t.Fatalf("target ignore did not control projection: %q %v", content, readErr)
			}
			if !reflect.DeepEqual(repo.appearanceIgnoreLines, test.currentIgnore) {
				t.Fatal("checkout retained its temporary target ignore policy")
			}
			if test.fail {
				rules, readErr := os.ReadFile(repo.absPath(repo.ignoreRulePath))
				if readErr != nil || !bytes.Equal(rules, []byte(currentRules)) ||
					repo.assetDownloads != nil && repo.assetDownloads.state.Pending != nil {
					t.Fatal("unknown target rule format changed current rules or created pending apply")
				}
			}
		})
	}
}
