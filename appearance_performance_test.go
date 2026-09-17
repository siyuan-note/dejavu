package dejavu

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/siyuan-note/dejavu/cloud"
)

// BenchmarkAppearanceHistory 衡量十个完整大包版本对无变化检查和笔记快照的影响，常规测试不运行此基准。
func BenchmarkAppearanceHistory(b *testing.B) {
	root := b.TempDir()
	repoPath := filepath.Join(root, "repo")
	remote := cloud.NewLocal(&cloud.BaseCloud{Conf: &cloud.Conf{
		Dir: "main", RepoPath: repoPath, AvailableSize: 1 << 30,
		Local: &cloud.ConfLocal{Endpoint: filepath.Join(root, "cloud")},
	}})
	repo, err := NewRepoWithOptions(Options{
		DataPath: filepath.Join(root, "data"), RepoPath: repoPath,
		HistoryPath: filepath.Join(root, "history"), TempPath: filepath.Join(root, "temp"),
		DeviceID: "appearance-benchmark", DeviceName: "Benchmark", DeviceOS: "windows",
		AESKey: []byte("0123456789abcdef0123456789abcdef"), Cloud: remote,
		EnableAppearanceSync: true,
		IgnoreLines:          []string{"/themes/", "/icons/", "/storage/bazaar/themes/", "/storage/bazaar/icons/"},
	})
	if err != nil {
		b.Fatal(err)
	}
	const versions, payloadSize = 10, 20 << 20
	content := make([]byte, payloadSize)
	var parent string
	var last *appearanceArchive
	for version := 0; version < versions; version++ {
		binary.LittleEndian.PutUint64(content, uint64(version))
		state, marshalErr := json.Marshal(map[string]interface{}{
			"version": 1, "deleted": false, "migration": false,
			"files": map[string]string{"font.bin": appearanceArchiveDigest(content)},
		})
		if marshalErr != nil {
			b.Fatal(marshalErr)
		}
		last = &appearanceArchive{Key: "/themes/performance", State: state,
			Files: map[string][]byte{"font.bin": content}}
		if parent != "" {
			last.Parents = []string{parent}
		}
		if _, err = repo.writeAppearanceArchive(last); err != nil {
			b.Fatal(err)
		}
		parent = last.Digest
	}
	for p, data := range map[string][]byte{
		"/themes/performance/font.bin": content,
		appearanceRecordPath(last.Key): last.State,
		"/note.txt":                    []byte("unchanged note"),
	} {
		if err = os.MkdirAll(filepath.Dir(repo.absPath(p)), 0755); err != nil {
			b.Fatal(err)
		}
		if err = os.WriteFile(repo.absPath(p), data, 0644); err != nil {
			b.Fatal(err)
		}
	}
	if _, err = repo.Index("appearance performance fixture", false, nil); err != nil {
		b.Fatal(err)
	}
	b.Run("CheckSnapshot", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			changed, checkErr := repo.CheckSnapshot()
			if checkErr != nil || changed {
				b.Fatalf("unchanged fixture differs: changed=%v err=%v", changed, checkErr)
			}
		}
	})
	b.Run("Index", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, _, indexErr := repo.IndexWithResult("unchanged appearance", false, nil); indexErr != nil {
				b.Fatal(indexErr)
			}
		}
	})
}
