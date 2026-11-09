package dejavu

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSnapshotScanDiagnosticUnknown(t *testing.T) {
	root := t.TempDir()
	repo := &Repo{DataPath: root}
	file := filepath.Join(root, "probe.txt")
	if err := os.WriteFile(file, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Lstat(file)
	detail, deterministic := repo.snapshotScanDiagnostic("/probe.txt", info)
	if deterministic || !strings.Contains(detail, "post-scan") || !strings.Contains(detail, "contains exact entry") {
		t.Fatalf("%v: %s", deterministic, detail)
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	detail, deterministic = repo.snapshotScanDiagnostic("/probe.txt", info)
	if deterministic || !strings.Contains(detail, "lstat") {
		t.Fatalf("%v: %s", deterministic, detail)
	}
}

func TestSnapshotScanDiagnosticAncestorLink(t *testing.T) {
	root := t.TempDir()
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(target, "probe.txt"), []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "linked")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	repo := &Repo{DataPath: root}
	info, _ := os.Lstat(filepath.Join(root, "linked", "probe.txt"))
	detail, deterministic := repo.snapshotScanDiagnostic("/linked/probe.txt", info)
	if !deterministic || !strings.Contains(detail, "ancestor link") {
		t.Fatalf("%v: %s", deterministic, detail)
	}
}

func TestSnapshotScanDiagnosticCaseInsensitiveSpelling(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "MixedDir"), 0700); err != nil {
		t.Fatal(err)
	}
	actual := filepath.Join(root, "MixedDir", "Probe.txt")
	if err := os.WriteFile(actual, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/MixedDir/probe.txt", "/mixeddir/Probe.txt"} {
		info, err := os.Lstat(filepath.Join(root, path))
		if err != nil {
			t.Skip("requires a case-insensitive filesystem")
		}
		repo := &Repo{DataPath: root}
		detail, deterministic := repo.snapshotScanDiagnostic(path, info)
		if !deterministic || !strings.Contains(detail, "same file identity verified") {
			t.Fatalf("%v: %s", deterministic, detail)
		}
	}
}

func TestSnapshotScanEntrySummaryBounded(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 30; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("%02d-%s", i, strings.Repeat("x", 120))), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	detail := snapshotScanEntrySummary(entries)
	if !strings.Contains(detail, "30 total, 16 shown") || strings.Contains(detail, strings.Repeat("x", 81)) || len(detail) > 2000 {
		t.Fatal(detail)
	}
}

func TestSnapshotScanDiagnosticDoesNotInferCaseAlias(t *testing.T) {
	root := t.TempDir()
	actual := filepath.Join(root, "Probe.txt")
	if err := os.WriteFile(actual, []byte("first"), 0600); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(root, "probe.txt")
	first, err := os.Lstat(actual)
	if err != nil {
		t.Fatal(err)
	}
	if second, e := os.Lstat(other); e == nil && os.SameFile(first, second) {
		t.Skip("requires a case-sensitive filesystem")
	}
	if err = os.WriteFile(other, []byte("second"), 0600); err != nil {
		t.Fatal(err)
	}
	second, err := os.Lstat(other)
	if err != nil {
		t.Fatal(err)
	}
	repo := &Repo{DataPath: root}
	detail, deterministic := repo.snapshotScanDiagnostic("/probe.txt", second)
	if deterministic || strings.Contains(detail, "spelling discrepancy") {
		t.Fatalf("distinct case-sensitive files were treated as aliases: %v %s", deterministic, detail)
	}
}
