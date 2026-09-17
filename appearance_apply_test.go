package dejavu

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/eventbus"
)

func newAppearancePendingTest(t *testing.T) (*Repo, *assetApply, string) {
	t.Helper()
	repo := newAssetTestRepo(t, t.TempDir(), t.TempDir(), "a", false)
	key := "/themes/example"
	writeAppearanceTestPackage(t, repo, key, map[string]string{"theme.json": "old manifest", "theme.css": "old css", "removed.css": "old removed"}, 10, false)
	oldIndex, err := repo.Index("old", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	oldFiles, err := repo.GetFiles(oldIndex)
	if err != nil {
		t.Fatal(err)
	}
	writeAppearanceTestPackage(t, repo, key, map[string]string{"theme.json": "new manifest", "theme.css": "new css", "assets/font.woff": "new font"}, 20, false)
	target, err := repo.Index("new", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	targetFiles, err := repo.GetFiles(target)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(repo.absPath(key)); err != nil {
		t.Fatal(err)
	}
	for _, file := range oldFiles {
		if appearancePackageKey(file.Path) != "" {
			if err = repo.checkoutFile(file, repo.DataPath, 1, 1, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = repo.UpdateLatest(oldIndex); err != nil {
		t.Fatal(err)
	}
	repo.appearanceSyncEnabled = true
	if err = repo.ensureAppearanceJournal(targetFiles); err != nil {
		t.Fatal(err)
	}
	upserts, removes := repo.diffUpsertRemove(targetFiles, oldFiles, false)
	pending := &assetApply{Index: target, Base: oldIndex, Upserts: upserts, Removes: removes,
		Before: map[string]*entity.File{}, Deferred: map[string]*entity.File{}}
	if err = repo.completeAppearanceApply(pending, filesByPath(targetFiles), filesByPath(oldFiles), nil); err != nil {
		t.Fatal(err)
	}
	repo.assetDownloads.state.Pending = pending
	if err = repo.saveAssetState(); err != nil {
		t.Fatal(err)
	}
	return repo, pending, key
}

func TestAppearanceRecoverPackageCommitBoundaries(t *testing.T) {
	for _, boundary := range []string{"journal", "partial stage", "old renamed", "new renamed", "record written"} {
		t.Run(boundary, func(t *testing.T) {
			repo, pending, key := newAppearancePendingTest(t)
			live, stage, backup, err := repo.appearanceTransactionPaths(pending.Index.ID, key)
			if err != nil {
				t.Fatal(err)
			}
			if boundary == "partial stage" {
				if err = os.MkdirAll(stage, 0755); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(filepath.Join(stage, "theme.css"), []byte("partial"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			if boundary == "old renamed" || boundary == "new renamed" || boundary == "record written" {
				if err = os.Rename(live, backup); err != nil {
					t.Fatal(err)
				}
			}
			if boundary == "new renamed" || boundary == "record written" {
				for _, file := range pending.Upserts {
					if strings.HasPrefix(file.Path, key+"/") || boundary == "record written" {
						if err = repo.checkoutFile(file, repo.DataPath, 1, 1, nil); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			// 重新绑定状态以覆盖进程重启后的恢复，而不是依赖内存中的暂存对象。
			if err = repo.ConfigureAssetDownloads(false, repo.assetDownloads.path, "test-scope"); err != nil {
				t.Fatal(err)
			}
			id, changes, err := repo.RecoverAssetDownloads(nil)
			if err != nil || id == "" || len(changes.Upserts) == 0 {
				t.Fatalf("recovery failed: %s %+v %v", id, changes, err)
			}
			if got := readAppearanceTestFile(t, repo, key+"/theme.css"); got != "new css" {
				t.Fatalf("mixed recovered package: %s", got)
			}
			if got := readAppearanceTestFile(t, repo, key+"/assets/font.woff"); got != "new font" {
				t.Fatalf("missing recovered asset: %s", got)
			}
			if _, err = os.Stat(repo.absPath(key + "/removed.css")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("obsolete file survived recovery: %v", err)
			}
			for _, p := range []string{stage, backup} {
				if _, err = os.Stat(p); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("transaction directory survived commit: %s %v", p, err)
				}
			}
			if err = repo.AcknowledgeAssetDownloadChanges(id); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAppearanceRecoveryPreservesExternalChanges(t *testing.T) {
	for _, p := range []string{"theme.css", "unexpected.txt"} {
		t.Run(p, func(t *testing.T) {
			repo, _, key := newAppearancePendingTest(t)
			writeAssetTestFile(t, repo, key+"/"+p, "external edit", 30)
			if _, _, err := repo.RecoverAssetDownloads(nil); !errors.Is(err, ErrIndexFileChanged) {
				t.Fatalf("external modification must stop recovery: %v", err)
			}
			if got := readAppearanceTestFile(t, repo, key+"/"+p); got != "external edit" {
				t.Fatalf("external edit was overwritten: %s", got)
			}
			if repo.assetDownloads.state.Pending == nil {
				t.Fatal("failed package recovery lost its journal")
			}
		})
	}
}

func TestAppearanceRecoveryPreservesIgnoredFiles(t *testing.T) {
	repo, _, key := newAppearancePendingTest(t)
	repo.IgnoreLines = []string{"themes/example/local.txt"}
	writeAssetTestFile(t, repo, key+"/local.txt", "ignored local file", 30)
	writeAssetTestFile(t, repo, key+"/.git/config", "development config", 30)
	if _, _, err := repo.RecoverAssetDownloads(nil); err != nil {
		t.Fatal(err)
	}
	for p, expected := range map[string]string{"local.txt": "ignored local file", ".git/config": "development config"} {
		if got := readAppearanceTestFile(t, repo, key+"/"+p); got != expected {
			t.Fatalf("ignored file was lost: %s %s", p, got)
		}
	}
}

func TestAppearanceIgnoredFileEditDuringStagingIsPreserved(t *testing.T) {
	repo, _, key := newAppearancePendingTest(t)
	repo.IgnoreLines = []string{"themes/example/local.txt"}
	writeAssetTestFile(t, repo, key+"/local.txt", "before", 30)
	token := new(int)
	context := map[string]interface{}{"appearance-staging-test": token}
	changed := false
	var writeErr error
	if err := eventbus.Subscribe(eventbus.EvtCheckoutUpsertFile, func(current map[string]interface{}, count, total int) {
		if current["appearance-staging-test"] != token || changed {
			return
		}
		changed = true
		writeErr = os.WriteFile(repo.absPath(key+"/local.txt"), []byte("during staging"), 0644)
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.RecoverAssetDownloads(context); !errors.Is(err, ErrIndexFileChanged) {
		t.Fatalf("ignored file race must stop package replacement: %v", err)
	}
	if writeErr != nil || !changed {
		t.Fatalf("race hook did not run: %v %v", changed, writeErr)
	}
	if got := readAppearanceTestFile(t, repo, key+"/local.txt"); got != "during staging" {
		t.Fatalf("ignored edit was lost: %s", got)
	}
	if got := readAppearanceTestFile(t, repo, key+"/theme.css"); got != "old css" {
		t.Fatalf("failed package was activated: %s", got)
	}
}

func TestAppearanceInstallAfterIndexDoesNotLeavePending(t *testing.T) {
	repo, pending, key := newAppearancePendingTest(t)
	repo.assetDownloads.state.Pending = nil
	if err := repo.saveAssetState(); err != nil {
		t.Fatal(err)
	}
	writeAppearanceTestPackage(t, repo, key, map[string]string{"theme.json": "installed manifest", "theme.css": "installed css"}, 30, false)
	if err := repo.preflightAppearanceApply(pending); !errors.Is(err, ErrIndexFileChanged) {
		t.Fatalf("new installation after index must stop old apply: %v", err)
	}
	if repo.assetDownloads.state.Pending != nil {
		t.Fatal("preflight failure left an unrecoverable pending journal")
	}
	if _, err := repo.Index("new installation", true, nil); err != nil {
		t.Fatalf("next operation could not index the preserved installation: %v", err)
	}
}

func TestAppearanceCheckoutAndManualDownload(t *testing.T) {
	base, remote := t.TempDir(), t.TempDir()
	a := newAssetTestRepo(t, base, remote, "a", false)
	b := newAssetTestRepo(t, base, remote, "b", false)
	key := "/themes/example"
	writeAppearanceTestPackage(t, a, key, map[string]string{"theme.json": "old manifest", "theme.css": "old css"}, 10, false)
	syncAppearanceTestRepo(t, a)
	syncAppearanceTestRepo(t, b)
	old, err := b.Latest()
	if err != nil {
		t.Fatal(err)
	}
	writeAppearanceTestPackage(t, a, key, map[string]string{"theme.json": "new manifest", "theme.css": "new css", "assets/font.woff": "font"}, 20, false)
	syncAppearanceTestRepo(t, a)
	writeAppearanceTestPackage(t, b, key, map[string]string{"theme.json": "local manifest", "theme.css": "local css"}, 30, false)
	if _, err = b.Index("local", true, nil); err != nil {
		t.Fatal(err)
	}
	result, _, err := b.SyncDownload(nil)
	if err != nil || len(result.HistoryPaths) != 3 {
		t.Fatalf("manual download must preserve complete local history: %+v %v", result, err)
	}
	if got := readAppearanceTestFile(t, b, key+"/theme.css"); got != "new css" {
		t.Fatalf("manual download used wrong package: %s", got)
	}
	if _, _, err = b.Checkout(old.ID, nil); err != nil {
		t.Fatal(err)
	}
	if got := readAppearanceTestFile(t, b, key+"/theme.css"); got != "old css" {
		t.Fatalf("checkout used wrong package: %s", got)
	}
	if _, err = os.Stat(b.absPath(key + "/assets/font.woff")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("checkout left a newer package file: %v", err)
	}
}

func TestAppearanceLegacyDeferredStateIsReadAndMaterialized(t *testing.T) {
	base, remote := t.TempDir(), t.TempDir()
	repo := newAssetTestRepo(t, base, remote, "a", true)
	key := "/themes/example"
	writeAppearanceTestPackage(t, repo, key, map[string]string{"theme.json": "manifest", "theme.css": "css", "assets/font.woff": "font"}, 10, false)
	syncAppearanceTestRepo(t, repo)
	file := assetTestFile(t, repo, key+"/assets/font.woff")
	repo.assetDownloads.state.Deferred[file.Path] = file
	if err := repo.saveAssetState(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(repo.absPath(file.Path)); err != nil {
		t.Fatal(err)
	}
	if err := repo.ConfigureAssetDownloads(true, repo.assetDownloads.path, "test-scope"); err != nil {
		t.Fatalf("legacy deferred record rejected: %v", err)
	}
	repo.appearanceSyncEnabled = true
	if _, _, err := repo.Sync(nil); err != nil {
		t.Fatal(err)
	}
	if got := readAppearanceTestFile(t, repo, file.Path); got != "font" {
		t.Fatalf("legacy package resource not materialized: %s", got)
	}
	if repo.assetDownloads.state.Deferred[file.Path] != nil {
		t.Fatal("legacy package resource remained deferred")
	}
}
