package dejavu

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
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

func TestAssetDownloadErrorContext(t *testing.T) {
	repo := newAssetTestRepo(t, t.TempDir(), t.TempDir(), "local", true)
	file := &entity.File{Path: "/assets/invalid:name.png", ID: strings.Repeat("a", 40)}
	err := repo.EnsureFileChunks(file, nil)
	if !errors.Is(err, ErrAssetDownloadState) || !strings.Contains(err.Error(), file.Path) || !strings.Contains(err.Error(), "reason=invalid path") {
		t.Fatalf("missing file context: %v", err)
	}
	statePath := repo.assetDownloads.path
	before, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	repo.assetDownloads.state.Scope = "changed"
	err = repo.reloadAssetState()
	if !errors.Is(err, ErrAssetDownloadState) || !strings.Contains(err.Error(), "sourceChanged=true") {
		t.Fatalf("missing source context: %v", err)
	}
	after, err := os.ReadFile(statePath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("rejected state was changed")
	}
	repo.assetDownloads.state.Pending = &assetApply{}
	err = repo.recoverAssetApply(nil)
	if !errors.Is(err, ErrAssetDownloadState) || !strings.Contains(err.Error(), "recover apply: incomplete pending record") {
		t.Fatalf("missing recovery context: %v", err)
	}
}

func TestCheckoutErrorApplyBoundary(t *testing.T) {
	repo := newPreflightTestRepo(t, t.TempDir(), t.TempDir(), "local", false)
	snapshot, err := repo.Latest()
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = repo.Checkout(strings.Repeat("f", 40), nil)
	var checkoutErr *CheckoutError
	if !errors.As(err, &checkoutErr) || checkoutErr.MayHaveApplied || checkoutErr.Stage != "target index" {
		t.Fatalf("index read classified as applied: %v", err)
	}
	writeAssetTestFile(t, repo, "/seed.txt", "changed", 2)
	rejected := errors.New("incompatible target")
	repo.SetCheckoutHooks(CheckoutHooks{Preflight: func(*CheckoutPlan) error { return rejected }})
	_, _, err = repo.Checkout(snapshot.ID, nil)
	if !errors.As(err, &checkoutErr) || checkoutErr.MayHaveApplied || !errors.Is(err, rejected) {
		t.Fatalf("preflight rejection classified as applied: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(repo.DataPath, "seed.txt"))
	if err != nil || string(data) != "changed" {
		t.Fatal("preflight changed current data")
	}
	repo.SetCheckoutHooks(CheckoutHooks{Before: func([]*entity.File) error { return rejected }})
	_, _, err = repo.Checkout(snapshot.ID, nil)
	if !errors.As(err, &checkoutErr) || !checkoutErr.MayHaveApplied || !errors.Is(err, rejected) {
		t.Fatalf("apply-phase error did not retain conservative state: %v", err)
	}
}
