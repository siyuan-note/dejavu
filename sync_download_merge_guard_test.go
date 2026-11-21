package dejavu

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// 仅下载中的结构化合并也必须经预检后落盘；拒绝预检时保留本地版本。
func TestSyncDownloadStructuredMergeRespectsPreflight(t *testing.T) {
	dir := t.TempDir()
	remote := filepath.Join(dir, "cloud")
	source := newAssetTestRepo(t, dir, remote, "source", false)
	target := newAssetTestRepo(t, dir, remote, "target", true)
	path := "/" + filepath.Base(filepath.Dir(dir)) + ".sy"
	base := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "second", "20240101000000"))
	local := syDoc(syPara(p1, "first", "20240101000000"), syPara(p2, "local", "20240102000000"))
	cloud := syDoc(syPara(p1, "cloud", "20240103000000"), syPara(p2, "second", "20240101000000"))
	want := mustMerge(t, base, local, cloud)
	writeAssetTestFile(t, source, path, string(base), 10)
	syncAssetTestRepo(t, source)
	syncAssetTestRepo(t, target)
	writeAssetTestFile(t, target, path, string(local), 12)
	if _, err := target.Index("local", true, nil); err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, source, path, string(cloud), 13)
	syncAssetTestRepo(t, source)
	rejected := errors.New("preflight rejected")
	called := false
	target.SetCheckoutHooks(CheckoutHooks{Preflight: func(plan *CheckoutPlan) error {
		called = true
		if got, err := plan.ReadFile(path); err != nil || !bytes.Equal(got, want) {
			t.Errorf("preflight received incorrect merged data: %v", err)
		}
		if got, err := os.ReadFile(target.absPath(path)); err != nil || !bytes.Equal(got, local) {
			t.Errorf("local data changed before preflight: %v", err)
		}
		return rejected
	}})
	if _, _, err := target.SyncDownload(nil); !errors.Is(err, rejected) {
		t.Fatalf("preflight rejection not returned: %v", err)
	}
	if !called {
		t.Fatal("preflight was not called")
	}
	if got, err := os.ReadFile(target.absPath(path)); err != nil || !bytes.Equal(got, local) {
		t.Fatalf("rejected download changed local data: %v", err)
	}
}
