package dejavu

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPurgeAfterSyncSourceBinding(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "purge-source", false)
	if err := repo.ConfigureSyncSource("source"); err != nil {
		t.Fatal(err)
	}
	syncAssetTestRepo(t, repo)
	tagged, err := repo.Latest()
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.AddTag(tagged.ID, "keep"); err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, repo, "/seed.txt", "unreferenced", 2)
	unused, err := repo.Index("unreferenced", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	writeAssetTestFile(t, repo, "/seed.txt", "latest", 3)
	syncAssetTestRepo(t, repo)
	if stat, err := repo.Purge(context.Background()); err != nil || stat.Indexes < 1 {
		t.Fatalf("purge after successful sync: %v", err)
	}
	if _, err = os.Stat(filepath.Join(repo.Path, "indexes", unused.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unreferenced snapshot remains: %v", err)
	}
	for _, ref := range []string{"latest", "latest-sync", "tags/keep"} {
		data, err := os.ReadFile(filepath.Join(repo.Path, "refs", filepath.FromSlash(ref)))
		if err != nil || !validSyncBaseline(string(data)) {
			t.Fatalf("invalid retained ref %s: %q %v", ref, data, err)
		}
		index, err := repo.store.GetIndex(string(data))
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range index.Files {
			file, err := repo.store.GetFile(id)
			if err != nil {
				t.Fatal(err)
			}
			for _, chunk := range file.Chunks {
				if _, err = repo.store.GetChunk(chunk); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if changed, err := repo.SyncSourceChanged(); err != nil || changed {
		t.Fatalf("purge changed binding: %v %v", changed, err)
	}
	if _, err := os.Stat(filepath.Join(repo.Path, "refs", "latest-sync-source")); !os.IsNotExist(err) {
		t.Fatalf("source metadata remains in refs: %v", err)
	}
}

func TestPurgeMigratesLegacySyncSource(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "interrupted"}[duplicate], func(t *testing.T) {
			base := t.TempDir()
			repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "legacy", false)
			index, err := repo.Latest()
			if err != nil {
				t.Fatal(err)
			}
			binding := syncSourceBinding{Version: 1, Source: "old-source", Baseline: index.ID}
			data, _ := json.Marshal(binding)
			legacyPath := filepath.Join(repo.Path, "refs", "latest-sync-source")
			if err = os.WriteFile(legacyPath, data, 0600); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(repo.Path, "sync-source.json")
			if duplicate {
				formatted, _ := json.MarshalIndent(binding, "", "  ")
				if err = os.WriteFile(path, formatted, 0600); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 2; i++ {
				if _, err = repo.Purge(context.Background()); err != nil {
					t.Fatal(err)
				}
				got, err := repo.readSyncSource()
				if err != nil || *got != binding {
					t.Fatalf("binding changed: %+v %v", got, err)
				}
				if _, err = os.Stat(legacyPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("legacy metadata remains: %v", err)
				}
			}
			got, err := os.ReadFile(path)
			if err != nil || (!duplicate && string(got) != string(data)) {
				t.Fatalf("migration changed original bytes: %q %v", got, err)
			}
		})
	}
}

func TestPurgePreservesInvalidSyncSourceBindings(t *testing.T) {
	valid := `{"version":1,"source":"source","baseline":"` + strings.Repeat("a", 40) + `"}`
	for _, invalid := range []string{
		`{`,
		`{"version":2,"source":"source","baseline":"` + strings.Repeat("a", 40) + `"}`,
		`{"version":1,"source":"","baseline":"` + strings.Repeat("a", 40) + `"}`,
		`{"version":1,"source":"source","baseline":"invalid"}`,
	} {
		for _, location := range []string{"new", "legacy", "new-with-legacy", "legacy-with-new"} {
			t.Run(location+"/"+invalid, func(t *testing.T) {
				base := t.TempDir()
				repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "invalid", false)
				path := filepath.Join(repo.Path, "sync-source.json")
				legacyPath := filepath.Join(repo.Path, "refs", "latest-sync-source")
				if strings.HasPrefix(location, "legacy") {
					path, legacyPath = legacyPath, path
				}
				if err := os.WriteFile(path, []byte(invalid), 0600); err != nil {
					t.Fatal(err)
				}
				if strings.Contains(location, "with") {
					if err := os.WriteFile(legacyPath, []byte(valid), 0600); err != nil {
						t.Fatal(err)
					}
				}
				before := purgeRepoFiles(t, repo)
				if _, err := repo.Purge(context.Background()); err == nil {
					t.Fatal("purge accepted invalid binding")
				}
				if !reflect.DeepEqual(before, purgeRepoFiles(t, repo)) {
					t.Fatal("failed purge changed recovery data")
				}
			})
		}
	}
}

func TestSyncSourceMigrationPreservesConflicts(t *testing.T) {
	for _, field := range []string{"source", "baseline"} {
		t.Run(field, func(t *testing.T) {
			base := t.TempDir()
			repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "conflict", false)
			binding := syncSourceBinding{Version: 1, Source: "source", Baseline: strings.Repeat("a", 40)}
			data, _ := json.Marshal(binding)
			if err := os.WriteFile(filepath.Join(repo.Path, "sync-source.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			if field == "source" {
				binding.Source = "different-source"
			} else {
				binding.Baseline = strings.Repeat("b", 40)
			}
			data, _ = json.Marshal(binding)
			if err := os.WriteFile(filepath.Join(repo.Path, "refs", "latest-sync-source"), data, 0600); err != nil {
				t.Fatal(err)
			}
			before := purgeRepoFiles(t, repo)
			if _, err := repo.Purge(context.Background()); err == nil {
				t.Fatal("purge accepted conflicting bindings")
			}
			if !reflect.DeepEqual(before, purgeRepoFiles(t, repo)) {
				t.Fatal("conflicting bindings changed recovery data")
			}
		})
	}
}

func TestSyncSourceMigrationRetainsLegacyOnIOError(t *testing.T) {
	for _, failure := range []string{"read", "write", "remove"} {
		t.Run(failure, func(t *testing.T) {
			base := t.TempDir()
			repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "io-error", false)
			data := []byte(`{"version":1,"source":"source","baseline":"` + strings.Repeat("a", 40) + `"}`)
			legacyPath := filepath.Join(repo.Path, "refs", "latest-sync-source")
			if err := os.WriteFile(legacyPath, data, 0600); err != nil {
				t.Fatal(err)
			}
			before := purgeRepoFiles(t, repo)
			path := filepath.Join(repo.Path, "sync-source.json")
			if failure == "read" {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			} else {
				dir := repo.Path
				if failure == "remove" {
					dir = filepath.Dir(legacyPath)
				}
				if err := os.Chmod(dir, 0500); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(dir, 0700) })
				probe := filepath.Join(dir, "permission-probe")
				if err := os.WriteFile(probe, nil, 0600); err == nil {
					_ = os.Remove(probe)
					t.Skip("filesystem does not enforce directory write permissions")
				}
			}
			if _, err := repo.Purge(context.Background()); err == nil {
				t.Fatal("purge accepted migration I/O error")
			}
			got, err := os.ReadFile(legacyPath)
			if err != nil || string(got) != string(data) {
				t.Fatalf("legacy binding lost: %q %v", got, err)
			}
			after := purgeRepoFiles(t, repo)
			delete(after, path)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("failed migration modified repository data")
			}
			if failure == "remove" {
				if err = os.Chmod(filepath.Dir(legacyPath), 0700); err != nil {
					t.Fatal(err)
				}
				if _, err = repo.Purge(context.Background()); err != nil {
					t.Fatalf("migration retry failed: %v", err)
				}
			}
		})
	}
}
