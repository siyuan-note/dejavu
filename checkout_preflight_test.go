package dejavu

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
)

const preflightNotebook = "/20261022000000-abcdefg"

func newPreflightTestRepo(t *testing.T, base, remote, device string, onDemand bool) *Repo {
	t.Helper()
	repo := newAssetTestRepo(t, base, remote, device, onDemand)
	repo.pathFilter = func(info os.FileInfo, absPath string) (bool, error) {
		if ignored, err := IgnorePath(info, absPath, repo.relPath(absPath), ".siyuan/syncignore", ".siyuan"); ignored || err != nil {
			return ignored, err
		}
		return info.IsDir(), nil
	}
	return repo
}

func TestCheckoutPreflightRejectsWholeSyncBatch(t *testing.T) {
	for _, mode := range []string{"legacy", "eager", "on-demand"} {
		for _, download := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/download=%t", mode, download), func(t *testing.T) {
				base := t.TempDir()
				remote := filepath.Join(base, "cloud")
				source := newPreflightTestRepo(t, base, remote, "source", false)
				target := newPreflightTestRepo(t, base, remote, "target", mode == "on-demand")
				if mode == "legacy" {
					source.assetDownloads, target.assetDownloads = nil, nil
				}
				config := preflightNotebook + "/.siyuan/conf.json"
				document := preflightNotebook + "/20261022000001-abcdefg.sy"
				removed := preflightNotebook + "/20261022000002-abcdefg.sy"
				created := preflightNotebook + "/20261022000003-abcdefg.sy"
				writeAssetTestFile(t, source, config, `{"documentAddressingVersion":1}`, 2)
				writeAssetTestFile(t, source, document, "old document", 2)
				writeAssetTestFile(t, source, removed, "keep until accepted", 2)
				syncAssetTestRepo(t, source)
				syncAssetTestRepo(t, target)
				previous := target.latestSync().ID
				writeAssetTestFile(t, source, config, `{"documentAddressingVersion":2}`, 3)
				writeAssetTestFile(t, source, document, "new document", 3)
				writeAssetTestFile(t, source, created, "new child", 3)
				if err := os.Remove(source.absPath(removed)); err != nil {
					t.Fatal(err)
				}
				syncAssetTestRepo(t, source)
				expected := errors.New("unsupported notebook mode")
				prepared, accepted, calls := false, false, 0
				var savedPlan *CheckoutPlan
				target.SetCheckoutHooks(CheckoutHooks{
					Apply: func() error { prepared = true; return nil },
					Preflight: func(plan *CheckoutPlan) error {
						calls++
						savedPlan = plan
						if !prepared || len(plan.Upserts) != 3 || len(plan.Removes) != 1 || plan.Removes[0].Path != removed {
							t.Fatalf("incomplete batch before preparation: %+v", plan)
						}
						data, err := plan.ReadFile(config)
						if err != nil || string(data) != `{"documentAddressingVersion":2}` {
							t.Fatalf("preflight did not read incoming configuration: %s %v", data, err)
						}
						if _, err = plan.ReadFile(removed); !errors.Is(err, os.ErrNotExist) {
							t.Fatalf("preflight read a removed file as incoming data: %v", err)
						}
						assertPreflightFiles(t, target, map[string]string{
							config: `{"documentAddressingVersion":1}`, document: "old document", removed: "keep until accepted",
						}, []string{created})
						if !accepted {
							return expected
						}
						return nil
					},
				})
				var err error
				if download {
					_, _, err = target.SyncDownload(nil)
				} else {
					_, _, err = target.Sync(nil)
				}
				if !errors.Is(err, expected) || calls != 1 || target.latestSync().ID != previous {
					t.Fatalf("preflight rejection applied changes or advanced sync: calls=%d err=%v", calls, err)
				}
				if _, err = savedPlan.ReadFile(config); err == nil {
					t.Fatal("expired plan remained readable")
				}
				assertPreflightFiles(t, target, map[string]string{
					config: `{"documentAddressingVersion":1}`, document: "old document", removed: "keep until accepted",
				}, []string{created})
				accepted = true
				if target.assetDownloads != nil {
					if err = target.ConfigureAssetDownloads(mode == "on-demand", target.assetDownloads.path, "test-scope"); err != nil {
						t.Fatal(err)
					}
					var recoveryID string
					recoveryID, _, err = target.RecoverAssetDownloads(nil)
					if err == nil {
						err = target.AcknowledgeAssetDownloadChanges(recoveryID)
					}
				} else if download {
					_, _, err = target.SyncDownload(nil)
				} else {
					_, _, err = target.Sync(nil)
				}
				if err != nil || calls != 2 {
					t.Fatalf("retry did not revalidate the complete batch: calls=%d err=%v", calls, err)
				}
				assertPreflightFiles(t, target, map[string]string{
					config: `{"documentAddressingVersion":2}`, document: "new document", created: "new child",
				}, []string{removed})
			})
		}
	}
}

func TestCheckoutPreflightProtectsSnapshotAndCloudFiles(t *testing.T) {
	for _, operation := range []string{"snapshot", "delete-only", "cloud-files"} {
		t.Run(operation, func(t *testing.T) {
			base := t.TempDir()
			repo := newPreflightTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
			config := preflightNotebook + "/.siyuan/conf.json"
			removed := preflightNotebook + "/20261022000002-abcdefg.sy"
			writeAssetTestFile(t, repo, config, `{"documentAddressingVersion":2}`, 2)
			snapshot, err := repo.Index("target", false, nil)
			if err != nil {
				t.Fatal(err)
			}
			file := assetTestFile(t, repo, config)
			current := `{"documentAddressingVersion":1}`
			if operation == "delete-only" {
				current = `{"documentAddressingVersion":2}`
			} else {
				writeAssetTestFile(t, repo, config, current, 3)
			}
			writeAssetTestFile(t, repo, removed, "retain", 3)
			expected := errors.New("mode rejected")
			calls := 0
			repo.SetCheckoutHooks(CheckoutHooks{Preflight: func(plan *CheckoutPlan) error {
				calls++
				if operation == "delete-only" {
					if len(plan.Upserts) != 0 || len(plan.Removes) != 1 {
						t.Fatalf("missing deletion-only batch: %+v", plan)
					}
				} else {
					data, readErr := plan.ReadFile(config)
					if readErr != nil || string(data) != `{"documentAddressingVersion":2}` {
						t.Fatalf("missing snapshot configuration: %s %v", data, readErr)
					}
				}
				return expected
			}})
			if operation == "cloud-files" {
				_, err = repo.CheckoutFilesFromCloud([]*entity.File{file}, nil)
			} else {
				_, _, err = repo.Checkout(snapshot.ID, nil)
			}
			if !errors.Is(err, expected) || calls != 1 {
				t.Fatalf("checkout bypassed batch preflight: calls=%d err=%v", calls, err)
			}
			assertPreflightFiles(t, repo, map[string]string{config: current, removed: "retain"}, nil)
			repo.SetCheckoutHooks(CheckoutHooks{Preflight: func(*CheckoutPlan) error { calls++; return nil }})
			if operation == "cloud-files" {
				_, err = repo.CheckoutFilesFromCloud([]*entity.File{file}, nil)
			} else {
				_, _, err = repo.Checkout(snapshot.ID, nil)
			}
			if err != nil || calls != 2 {
				t.Fatalf("accepted retry failed: calls=%d err=%v", calls, err)
			}
			present := map[string]string{config: `{"documentAddressingVersion":2}`}
			var absent []string
			if operation == "cloud-files" {
				present[removed] = "retain"
			} else {
				absent = []string{removed}
			}
			assertPreflightFiles(t, repo, present, absent)
		})
	}
}

func TestCheckoutPreflightIsolatesPlanAndAuthenticatesRead(t *testing.T) {
	base := t.TempDir()
	repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	file := assetTestFile(t, repo, "/seed.txt")
	path, chunk := file.Path, file.Chunks[0]
	repo.SetCheckoutHooks(CheckoutHooks{Preflight: func(plan *CheckoutPlan) error {
		plan.Upserts[0].Path = "/changed.txt"
		plan.Upserts[0].Chunks[0] = "changed-chunk"
		data, err := plan.ReadFile(path)
		if err != nil || string(data) != "seed" {
			t.Fatalf("callback mutated the source read: %s %v", data, err)
		}
		return nil
	}})
	if err := repo.preflightCheckout([]*entity.File{file}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if file.Path != path || file.Chunks[0] != chunk {
		t.Fatal("callback mutated the actual checkout plan")
	}
	wrongSize := *file
	wrongSize.Size++
	repo.SetCheckoutHooks(CheckoutHooks{Preflight: func(plan *CheckoutPlan) error {
		_, err := plan.ReadFile(path)
		return err
	}})
	if err := repo.preflightCheckout([]*entity.File{&wrongSize}, nil, nil); err == nil {
		t.Fatal("preflight accepted source bytes with a different authenticated size")
	}
	_, chunkPath := repo.store.AbsPath(chunk)
	encoded, err := os.ReadFile(chunkPath)
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] ^= 1
	if err = os.WriteFile(chunkPath, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	if err = repo.preflightCheckout([]*entity.File{file}, nil, nil); err == nil {
		t.Fatal("preflight accepted a chunk that failed authentication")
	}
	encoded, err = repo.store.encodeData([]byte("fake"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(chunkPath, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	if err = repo.preflightCheckout([]*entity.File{file}, nil, nil); err == nil {
		t.Fatal("preflight accepted authenticated bytes with a different chunk hash")
	}
}

func TestCheckoutPreflightCancellationKeepsWholeBatch(t *testing.T) {
	base := t.TempDir()
	repo := newPreflightTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
	file := assetTestFile(t, repo, "/seed.txt")
	writeAssetTestFile(t, repo, file.Path, "local edit", 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo.SetCheckoutHooks(CheckoutHooks{Context: ctx, Preflight: func(*CheckoutPlan) error {
		cancel()
		return nil
	}})
	if _, err := repo.CheckoutFilesFromCloud([]*entity.File{file}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("preflight ignored cancellation: %v", err)
	}
	assertPreflightFiles(t, repo, map[string]string{file.Path: "local edit"}, nil)
}

func assertPreflightFiles(t *testing.T, repo *Repo, present map[string]string, absent []string) {
	t.Helper()
	for path, want := range present {
		if data, err := os.ReadFile(repo.absPath(path)); err != nil || string(data) != want {
			t.Fatalf("unexpected file state %s: %q, %v", path, data, err)
		}
	}
	for _, path := range absent {
		if _, err := os.Stat(repo.absPath(path)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unexpected created file %s: %v", path, err)
		}
	}
}
