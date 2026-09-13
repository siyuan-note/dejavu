package dejavu

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOptionsIgnoreRulePath(t *testing.T) {
	for _, path := range []string{"../rules", "/rules", "a/../rules", "a\\rules", ".", "C:/rules"} {
		if _, err := NewRepoWithOptions(Options{IgnoreRulePath: path}); err == nil {
			t.Fatalf("accepted invalid path %q", path)
		}
	}
	base := t.TempDir()
	repo, err := NewRepoWithOptions(Options{DataPath: filepath.Join(base, "data"), RepoPath: filepath.Join(base, "repo"),
		HistoryPath: filepath.Join(base, "history"), TempPath: filepath.Join(base, "temp"), AESKey: []byte("0123456789abcdef0123456789abcdef"), IgnoreRulePath: ".settings/rules",
		PathFilter: func(os.FileInfo, string) (bool, error) { return false, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, repo, "/.settings/rules", "/assets/ignored.bin", 100)
	writeAssetTestFile(t, repo, "/.hidden/file", "hidden", 100)
	writeAssetTestFile(t, repo, "/private/.settings/secret.txt", "private", 100)
	index, err := repo.Index("custom rules", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	files, err := repo.GetFiles(index)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "/.settings/rules" {
		t.Fatalf("unexpected files: %v", files)
	}
	matcher, err := repo.cloudAssetIgnoreMatcher(files, nil)
	if err != nil || !matcher.MatchesPath("/assets/ignored.bin") {
		t.Fatalf("custom cloud rules not read: %v", err)
	}
	info, err := os.Stat(filepath.Join(repo.DataPath, ".hidden"))
	if err != nil {
		t.Fatal(err)
	}
	if ignored, skip := repo.builtInIgnore(info, filepath.Join(repo.DataPath, ".hidden")); !ignored || skip != filepath.SkipDir {
		t.Fatal("hidden directory was not pruned")
	}
}
