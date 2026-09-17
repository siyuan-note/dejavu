package dejavu

import (
	"os"
	"syscall"
	"testing"
)

func TestAppearanceRecoveryPreservesWindowsHiddenFile(t *testing.T) {
	repo, _, key := newAppearancePendingTest(t)
	p := repo.absPath(key + "/desktop.ini")
	if err := os.WriteFile(p, []byte("hidden settings"), 0644); err != nil {
		t.Fatal(err)
	}
	ptr, err := syscall.UTF16PtrFromString(p)
	if err != nil {
		t.Fatal(err)
	}
	if err = syscall.SetFileAttributes(ptr, syscall.FILE_ATTRIBUTE_HIDDEN); err != nil {
		t.Fatal(err)
	}
	if _, _, err = repo.RecoverAssetDownloads(nil); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(p)
	if err != nil || !hiddenFile(info, p) {
		t.Fatalf("hidden file attributes were lost: %v", err)
	}
	if got := readAppearanceTestFile(t, repo, key+"/desktop.ini"); got != "hidden settings" {
		t.Fatalf("hidden file contents were lost: %s", got)
	}
}
