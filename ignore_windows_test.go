package dejavu

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestIgnoreWindowsHiddenAttributes(t *testing.T) {
	base := t.TempDir()
	for _, directory := range []bool{false, true} {
		name := filepath.Join(base, "entry")
		if directory {
			if err := os.Mkdir(name, 0755); err != nil {
				t.Fatal(err)
			}
		} else if err := os.WriteFile(name, []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}
		ptr, err := syscall.UTF16PtrFromString(name)
		if err != nil {
			t.Fatal(err)
		}
		if err = syscall.SetFileAttributes(ptr, syscall.FILE_ATTRIBUTE_HIDDEN); err != nil {
			t.Fatal(err)
		}
		info, err := os.Lstat(name)
		if err != nil {
			t.Fatal(err)
		}
		if !hiddenFile(info, filepath.Join(base, "missing")) {
			t.Fatal("hidden attributes were not read from file info")
		}
		ignored, skip := IgnorePath(info, name, "/entry", "")
		if ignored == directory || skip != nil {
			t.Fatalf("hidden attribute compatibility changed: %v %v", ignored, skip)
		}
		if err = syscall.SetFileAttributes(ptr, syscall.FILE_ATTRIBUTE_NORMAL); err != nil {
			t.Fatal(err)
		}
		if err = os.Remove(name); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHiddenDirectoryUpgradePreservesSnapshot(t *testing.T) {
	base := t.TempDir()
	data, repoPath := filepath.Join(base, "data"), filepath.Join(base, "repo")
	key := []byte("0123456789abcdef0123456789abcdef")
	legacy, err := NewRepo(data, repoPath, filepath.Join(base, "history"), filepath.Join(base, "temp"),
		"device", "device", "windows", key, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, legacy, "/notebook/doc.sy", "existing document", 10)
	writeAssetTestFile(t, legacy, "/seed.txt", "seed", 10)
	ptr, err := syscall.UTF16PtrFromString(filepath.Join(data, "notebook"))
	if err != nil {
		t.Fatal(err)
	}
	if err = syscall.SetFileAttributes(ptr, syscall.FILE_ATTRIBUTE_HIDDEN); err != nil {
		t.Fatal(err)
	}
	before, err := legacy.Index("before upgrade", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	next, err := NewRepoWithOptions(Options{DataPath: data, RepoPath: repoPath, HistoryPath: filepath.Join(base, "history"),
		TempPath: filepath.Join(base, "temp"), AESKey: key})
	if err != nil {
		t.Fatal(err)
	}
	after, err := next.Index("after upgrade", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Files) != 2 || len(after.Files) != 2 {
		t.Fatal("hidden directory disappeared during upgrade")
	}
	if err = syscall.SetFileAttributes(ptr, syscall.FILE_ATTRIBUTE_NORMAL); err != nil {
		t.Fatal(err)
	}
	if _, removes, err := legacy.Checkout(after.ID, nil); err != nil || len(removes) != 0 {
		t.Fatalf("peer checkout removed existing files: %v %v", removes, err)
	}
	content, err := os.ReadFile(filepath.Join(data, "notebook", "doc.sy"))
	if err != nil || string(content) != "existing document" {
		t.Fatalf("existing document changed: %q %v", content, err)
	}
}
