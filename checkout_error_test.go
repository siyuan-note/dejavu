package dejavu

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckoutRenameErrorPreservesDestination(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	file := assetTestFile(t, repo, "/seed.txt")
	destination := filepath.Join(base, "checkout")
	occupied := filepath.Join(destination, "seed.txt")
	if err := os.MkdirAll(occupied, 0755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(occupied, "keep.txt")
	if err := os.WriteFile(marker, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := repo.checkoutFile(file, destination, 1, 1, nil); err == nil {
		t.Fatal("expected rename failure")
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "keep" {
		t.Fatalf("destination changed: %q, %v", data, err)
	}
	entries, err := os.ReadDir(destination)
	if err != nil || len(entries) != 1 {
		t.Fatalf("temporary checkout file leaked: %v, %v", entries, err)
	}
}
