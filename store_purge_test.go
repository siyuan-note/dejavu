package dejavu

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func purgeRepoFiles(t *testing.T, repo *Repo) map[string]string {
	t.Helper()
	ret := map[string]string{}
	if err := filepath.WalkDir(repo.Path, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		ret[path] = string(data)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return ret
}

func TestPurgePreservesDataWithMalformedRefs(t *testing.T) {
	for _, ref := range []string{"latest", "tags/keep"} {
		for _, content := range []string{"", "truncated", strings.Repeat("a", 43), strings.Repeat("g", 40)} {
			t.Run(ref+"/"+content, func(t *testing.T) {
				base := t.TempDir()
				repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "purge-refs", false)
				old, err := repo.Latest()
				if err != nil {
					t.Fatal(err)
				}
				if err := repo.AddTag(old.ID, "keep"); err != nil {
					t.Fatal(err)
				}
				writeAssetTestFile(t, repo, "/seed.txt", "new version", 2)
				if _, err := repo.Index("new", true, nil); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(repo.Path, "refs", filepath.FromSlash(ref)), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
				before := purgeRepoFiles(t, repo)
				if _, err := repo.Purge(context.Background()); err == nil {
					t.Fatal("purge accepted an incomplete retention-root set")
				}
				if !reflect.DeepEqual(purgeRepoFiles(t, repo), before) {
					t.Fatal("failed purge modified recovery data")
				}
			})
		}
	}
}

type purgeCancelOnCheck struct {
	context.Context
	cancel    context.CancelFunc
	remaining int
}

func (c *purgeCancelOnCheck) Done() <-chan struct{} {
	c.remaining--
	if c.remaining == 0 {
		c.cancel()
	}
	return c.Context.Done()
}

func TestPurgeStopsScanningFilesInRetainedIndexOnCancellation(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "purge-cancel", false)
	index, err := repo.Latest()
	if err != nil {
		t.Fatal(err)
	}
	index.Files = append(index.Files, strings.Repeat("0", 40))
	if err := repo.PutIndex(index); err != nil {
		t.Fatal(err)
	}
	before := purgeRepoFiles(t, repo)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 在已检查对象目录、索引目录、保留索引和第一个文件后取消，第二个文件不存在。
	scan := &purgeCancelOnCheck{Context: ctx, cancel: cancel, remaining: 5}
	if _, err := repo.Purge(scan); err != nil || ctx.Err() != context.Canceled {
		t.Fatalf("purge did not stop before reading the second file: %v, context error: %v", err, ctx.Err())
	}
	if !reflect.DeepEqual(purgeRepoFiles(t, repo), before) {
		t.Fatal("canceled purge modified the repository")
	}
}

func TestReadRefsAcceptsExistingLineEndings(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "ref-lines", false)
	index, err := repo.Latest()
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "\n", "\r\n"} {
		if err := os.WriteFile(filepath.Join(repo.Path, "refs", "latest"), []byte(index.ID+suffix), 0600); err != nil {
			t.Fatal(err)
		}
		refs, err := repo.store.readRefs()
		if err != nil || !refs[index.ID] {
			t.Fatalf("existing reference line ending rejected: %q, %v", suffix, err)
		}
	}
}
