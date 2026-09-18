package dejavu

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
)

func TestPackageFilesSyncAndSnapshot(t *testing.T) {
	base := t.TempDir()
	remote := filepath.Join(base, "cloud")
	full := newAssetTestRepo(t, base, remote, "full", false)
	before, err := full.Latest()
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{"/themes/example/theme.css", "/themes/example/assets/font.woff", "/icons/example/icon.js", "/icons/example/assets/icon.png", "/plugins/example/assets/run.js", "/widgets/example/assets/run.js", "/templates/example/assets/image.png"}
	for _, p := range paths {
		writeAssetTestFile(t, full, p, p, 2)
	}
	writeAssetTestFile(t, full, "/assets/deferred.bin", "deferred", 2)
	syncAssetTestRepo(t, full)
	partial := newAssetTestRepo(t, base, remote, "partial", true)
	syncAssetTestRepo(t, partial)
	for _, p := range paths {
		if data, err := os.ReadFile(partial.absPath(p)); err != nil || string(data) != p {
			t.Fatalf("package file not downloaded: %s: %q, %v", p, data, err)
		}
	}
	if deferred, err := partial.DeferredAssets(); err != nil || len(deferred) != 1 || deferred[0].Path != "/assets/deferred.bin" {
		t.Fatalf("unexpected deferred assets: %v, %v", deferred, err)
	}
	// 同一主题中的不同文件分别合并，不依赖整包版本或状态记录。
	writeAssetTestFile(t, full, paths[0], "local css", 3)
	writeAssetTestFile(t, partial, paths[1], "remote font", 4)
	syncAssetTestRepo(t, full)
	syncAssetTestRepo(t, partial)
	syncAssetTestRepo(t, full)
	for p, want := range map[string]string{paths[0]: "local css", paths[1]: "remote font"} {
		if data, err := os.ReadFile(full.absPath(p)); err != nil || string(data) != want {
			t.Fatalf("file-level merge failed: %s: %q, %v", p, data, err)
		}
	}
	for _, p := range []string{"storage/appearance-v1", "storage/bazaar/themes", "storage/bazaar/icons", ".siyuan/syncignore"} {
		if _, err := os.Stat(full.absPath(p)); !os.IsNotExist(err) {
			t.Fatalf("unexpected appearance protocol artifact: %s: %v", p, err)
		}
	}
	if _, _, err = full.Checkout(before.ID, nil); err != nil {
		t.Fatal(err)
	}
	for _, p := range paths {
		if _, err := os.Stat(full.absPath(p)); !os.IsNotExist(err) {
			t.Fatalf("pre-package snapshot retained %s: %v", p, err)
		}
	}
	syncAssetTestRepo(t, full)
	syncAssetTestRepo(t, partial)
	for _, p := range paths {
		if _, err := os.Stat(partial.absPath(p)); !os.IsNotExist(err) {
			t.Fatalf("package deletion not synchronized: %s: %v", p, err)
		}
	}
	// 历史内部标签作为普通标签保留，用户可以显式删除。
	if err = full.AddTag(before.ID, ".siyuan-appearance-v1"); err != nil {
		t.Fatal(err)
	}
	if err = full.RemoveTag(".siyuan-appearance-v1"); err != nil {
		t.Fatal(err)
	}
}

func TestAssetDownloadsUpgradeLegacyPackageState(t *testing.T) {
	for _, operation := range []string{"index", "sync", "download", "upload", "local-edit", "offline", "pending"} {
		t.Run(operation, func(t *testing.T) {
			base := t.TempDir()
			remote := filepath.Join(base, "cloud")
			full := newAssetTestRepo(t, base, remote, "full", false)
			p := "/plugins/example/assets/runtime.js"
			writeAssetTestFile(t, full, p, "runtime", 2)
			writeAssetTestFile(t, full, "/assets/deferred.bin", "keep deferred", 2)
			syncAssetTestRepo(t, full)
			partial := newAssetTestRepo(t, base, remote, "partial", true)
			syncAssetTestRepo(t, partial)
			file := assetTestFile(t, partial, p)
			// 构造旧版已认证的延期清单，并移除尚未下载的包文件及分块。
			partial.assetDownloads.state.Deferred[p] = file
			if err := partial.saveAssetState(); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(partial.absPath(p)); err != nil {
				t.Fatal(err)
			}
			for _, id := range file.Chunks {
				if err := os.Remove(filepath.Join(partial.Path, "objects", id[:2], id[2:])); err != nil {
					t.Fatal(err)
				}
			}
			if err := partial.ConfigureAssetDownloads(true, partial.assetDownloads.path, "test-scope"); err != nil {
				t.Fatalf("legacy state rejected: %v", err)
			}
			if files, err := ReadDeferredAssets(partial.assetDownloads.path, partial.store.AesKey); err != nil || len(files) != 2 {
				t.Fatalf("legacy readonly state rejected: %v, %v", files, err)
			}
			want := "runtime"
			var err error
			switch operation {
			case "pending":
				index, latestErr := partial.Latest()
				if latestErr != nil {
					t.Fatal(latestErr)
				}
				partial.assetDownloads.state.Pending = &assetApply{Index: index, Base: index,
					Deferred: partial.assetDownloads.state.Deferred, Before: map[string]*entity.File{}}
				if err = partial.saveAssetState(); err != nil {
					t.Fatal(err)
				}
				if _, _, err = partial.RecoverAssetDownloads(nil); err != nil {
					t.Fatalf("legacy pending state rejected: %v", err)
				}
				_, _, err = partial.Sync(nil)
			case "sync":
				_, _, err = partial.Sync(nil)
			case "download":
				_, _, err = partial.SyncDownload(nil)
			case "upload":
				_, err = partial.SyncUpload(nil)
			case "offline":
				_, err = partial.Index("offline", true, map[string]interface{}{CtxAssetDownloadsAllowed: false})
				if err == nil || partial.assetDownloads.state.Deferred[p] == nil {
					t.Fatalf("offline upgrade lost pending file: %v", err)
				}
				_, err = partial.Index("retry", true, nil)
			default:
				if operation == "local-edit" {
					want = "local edit"
					writeAssetTestFile(t, partial, p, want, 3)
				}
				_, err = partial.Index("upgrade", true, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			if data, err := os.ReadFile(partial.absPath(p)); err != nil || string(data) != want {
				t.Fatalf("legacy package not restored: %q, %v", data, err)
			}
			if files, err := partial.DeferredAssets(); err != nil || len(files) != 1 || files[0].Path != "/assets/deferred.bin" {
				t.Fatalf("unexpected upgraded state: %v, %v", files, err)
			}
		})
	}
}
