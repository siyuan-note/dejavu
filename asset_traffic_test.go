package dejavu

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/siyuan-note/dejavu/cloud"
)

type assetTrafficCloud struct {
	cloud.Cloud
	downloads atomic.Int32
	reports   chan *cloud.Traffic
	fail      bool
}

func (c *assetTrafficCloud) DownloadObject(p string) ([]byte, error) {
	c.downloads.Add(1)
	if c.fail {
		return nil, errors.New("download unavailable")
	}
	return c.Cloud.DownloadObject(p)
}

func (c *assetTrafficCloud) AddTraffic(traffic *cloud.Traffic) { c.reports <- traffic }

func TestAssetDownloadsTraffic(t *testing.T) {
	for _, mode := range []string{"current", "history", "all", "snapshots", "peer", "failure"} {
		t.Run(mode, func(t *testing.T) {
			base := t.TempDir()
			remote := filepath.Join(base, "cloud")
			full := newAssetTestRepo(t, base, remote, "full", false)
			writeAssetTestFile(t, full, "/assets/first.bin", "first resource", 90)
			writeAssetTestFile(t, full, "/assets/second.bin", "second resource", 90)
			syncAssetTestRepo(t, full)
			partial := newAssetTestRepo(t, base, remote, "partial", true)
			syncAssetTestRepo(t, partial)
			spy := &assetTrafficCloud{Cloud: partial.cloud, reports: make(chan *cloud.Traffic, 10), fail: mode == "failure"}
			partial.cloud = spy
			file := assetTestFile(t, partial, "/assets/first.bin")
			if mode == "peer" {
				source := &testChunkSource{chunks: map[string][]byte{}}
				files, err := partial.DeferredAssets()
				if err != nil {
					t.Fatal(err)
				}
				for _, file := range files {
					for _, id := range file.Chunks {
						_, p := full.store.AbsPath(id)
						data, readErr := os.ReadFile(p)
						if readErr != nil {
							t.Fatal(readErr)
						}
						source.chunks[id] = data
					}
				}
				partial.SetChunkSource(source)
			}
			download := func() error {
				switch mode {
				case "current", "failure":
					_, err := partial.EnsureAsset(file.Path, nil)
					return err
				case "history":
					return partial.EnsureFileChunks(file, nil)
				case "snapshots":
					return partial.EnsureAllSnapshotChunks(nil)
				default:
					return partial.EnsureAllAssets(nil)
				}
			}
			err := download()
			if (mode == "failure") != (err != nil) {
				t.Fatalf("download result: %v", err)
			}
			if mode != "peer" {
				select {
				case report := <-spy.reports:
					if report.APIGet != int(spy.downloads.Load()) || report.APIGet == 0 {
						t.Fatalf("incorrect cloud requests: %+v, actual %d", report, spy.downloads.Load())
					}
					if mode != "failure" && report.DownloadBytes <= 0 {
						t.Fatalf("missing download bytes: %+v", report)
					}
				case <-time.After(time.Second):
					t.Fatal("download traffic was not reported")
				}
			} else if spy.downloads.Load() != 0 {
				t.Fatal("peer download used cloud")
			}
			if mode != "failure" {
				if err = download(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case report := <-spy.reports:
				t.Fatalf("duplicate, local, or peer traffic was reported: %+v", report)
			case <-time.After(30 * time.Millisecond):
			}
		})
	}
}
