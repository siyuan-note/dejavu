package dejavu

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestWorkspaceJunctionSync(t *testing.T) {
	for _, mode := range []string{"automatic", "on-demand", "one-way"} {
		t.Run(mode, func(t *testing.T) {
			base := t.TempDir()
			remote := filepath.Join(base, "remote")
			peer := newAssetTestRepo(t, base, remote, "peer", false)
			local := newAssetTestRepo(t, base, remote, "local", mode == "on-demand")
			if mode != "on-demand" {
				local.assetDownloads = nil
				peer.assetDownloads = nil
			}
			const tracked = "/plugins/dev-plugin/plugin.json"
			const nearby = "/plugins/dev-plugin-other/plugin.json"
			writeAssetTestFile(t, peer, tracked, "original from peer", 2)
			syncJunctionTestRepo(t, peer)
			syncJunctionTestRepo(t, local)
			assertJunctionTestFile(t, local.absPath(tracked), "original from peer")

			link := filepath.Dir(local.absPath(tracked))
			target := filepath.Join(base, "external")
			if err := os.Rename(link, target); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(target, "plugin.json"), []byte("external file"), 0600); err != nil {
				t.Fatal(err)
			}
			if output, err := exec.Command("cmd.exe", "/d", "/c", "mklink", "/J", link, target).CombinedOutput(); err != nil {
				t.Fatalf("create junction: %v (%s)", err, output)
			}
			t.Cleanup(func() {
				if err := os.Remove(link); err != nil {
					t.Error(err)
				}
			})

			changed, err := local.CheckSnapshot()
			if err != nil || !changed {
				t.Fatalf("detect junction conversion: %v, %v", changed, err)
			}
			writeAssetTestFile(t, local, nearby, "adjacent regular plugin", 3)
			snapshot, err := local.Index("with junction", false, nil)
			if err != nil {
				t.Fatal(err)
			}
			files, err := local.GetFiles(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if filesByPath(files)[tracked] != nil {
				t.Fatal("local snapshot retained the junction contents")
			}
			if mode == "one-way" {
				if _, err := local.SyncUpload(nil); err != nil {
					t.Fatal(err)
				}
			} else {
				syncJunctionTestRepo(t, local)
			}
			syncJunctionTestRepo(t, peer)
			assertJunctionTestFile(t, peer.absPath(tracked), "original from peer")
			assertJunctionTestFile(t, peer.absPath(nearby), "adjacent regular plugin")

			writeAssetTestFile(t, peer, tracked, "updated by peer", 4)
			writeAssetTestFile(t, peer, "/plugins/dev-plugin/new.txt", "cloud only", 4)
			writeAssetTestFile(t, peer, nearby, "new adjacent version", 4)
			syncJunctionTestRepo(t, peer)
			if mode == "one-way" {
				if _, _, err := local.SyncDownload(nil); err != nil {
					t.Fatal(err)
				}
			} else {
				syncJunctionTestRepo(t, local)
			}
			assertJunctionTestFile(t, local.absPath(nearby), "new adjacent version")
			assertJunctionTestFile(t, filepath.Join(target, "plugin.json"), "external file")
			if _, err := os.Lstat(filepath.Join(target, "new.txt")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("sync accessed the junction target: %v", err)
			}
			assertJunctionTestFile(t, peer.absPath(tracked), "updated by peer")
		})
	}
}

func syncJunctionTestRepo(t *testing.T, repo *Repo) {
	t.Helper()
	if repo.usesAssetDownloads() {
		syncAssetTestRepo(t, repo)
		return
	}
	if _, err := repo.Index("sync", false, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.Sync(nil); err != nil {
		t.Fatal(err)
	}
}

func assertJunctionTestFile(t *testing.T, name, expected string) {
	t.Helper()
	got, err := os.ReadFile(name)
	if err != nil || string(got) != expected {
		t.Fatalf("read %s: got %q, want %q (%v)", name, got, expected, err)
	}
}
