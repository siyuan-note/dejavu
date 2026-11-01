package dejavu

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
	"github.com/siyuan-note/logging"
)

func TestEnsureFileChunksSizeDiagnostics(t *testing.T) {
	for _, delta := range []int64{-3, 0, 3} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			base := t.TempDir()
			repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
			content := []byte("private-document-content")
			chunk := &entity.Chunk{ID: util.Hash(content), Data: content}
			if err := repo.store.PutChunk(chunk); err != nil {
				t.Fatal(err)
			}
			actual := int64(2 * len(content))
			file := entity.NewFile("/20230907102058-34od74e/20231214105701-n5uff2f.sy", actual+delta, 1702522745000)
			file.Chunks = []string{chunk.ID, chunk.ID}
			if err := repo.store.PutFile(file); err != nil {
				t.Fatal(err)
			}
			_, chunkPath := repo.store.AbsPath(chunk.ID)
			_, filePath := repo.store.AbsPath(file.ID)
			before := map[string][]byte{}
			for _, path := range []string{chunkPath, filePath} {
				var err error
				before[path], err = os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
			}
			previousLogPath := logging.LogPath
			logPath := filepath.Join(base, "diagnostics.log")
			logging.SetLogPath(logPath)
			t.Cleanup(func() { logging.SetLogPath(previousLogPath) })
			err := repo.EnsureFileChunks(file, nil)
			if delta == 0 {
				if err != nil {
					t.Fatal(err)
				}
				if _, err = os.Stat(logPath); !os.IsNotExist(err) {
					t.Fatalf("valid chunks produced diagnostics: %v", err)
				}
			} else {
				if err == nil || err.Error() != "resource size mismatch: "+file.Path {
					t.Fatalf("unexpected public error: %v", err)
				}
				data, readErr := os.ReadFile(logPath)
				if readErr != nil {
					t.Fatal(readErr)
				}
				expected := fmt.Sprintf("resource size mismatch [path=%s, fileID=%s, updated=%d, expectedSize=%d, actualSize=%d, chunkCount=2]",
					file.Path, file.ID, file.Updated, file.Size, actual)
				if !strings.Contains(string(data), expected) || bytes.Contains(data, content) || bytes.Contains(data, repo.store.AesKey) {
					t.Fatalf("missing metadata diagnostics or leaked document/key: %s", data)
				}
			}
			if file.Size != actual+delta || len(file.Chunks) != 2 || file.Chunks[0] != chunk.ID || file.Chunks[1] != chunk.ID {
				t.Fatal("diagnostics changed file metadata")
			}
			for path, original := range before {
				after, readErr := os.ReadFile(path)
				if readErr != nil || !bytes.Equal(original, after) {
					t.Fatalf("diagnostics changed stored object: %v", readErr)
				}
			}
		})
	}
}

func TestEnsureFileChunksSizeDiagnosticsRequireAuthenticatedChunks(t *testing.T) {
	for _, validCiphertext := range []bool{false, true} {
		t.Run(fmt.Sprint(validCiphertext), func(t *testing.T) {
			base := t.TempDir()
			repo := newAssetTestRepo(t, base, filepath.Join(base, "cloud"), "device", false)
			file := *assetTestFile(t, repo, "/seed.txt")
			file.Size++
			_, chunkPath := repo.store.AbsPath(file.Chunks[0])
			data := []byte("invalid ciphertext")
			if validCiphertext {
				var err error
				data, err = repo.store.encodeData([]byte("different authenticated content"))
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(chunkPath, data, 0600); err != nil {
				t.Fatal(err)
			}
			previousLogPath := logging.LogPath
			logPath := filepath.Join(base, "diagnostics.log")
			logging.SetLogPath(logPath)
			t.Cleanup(func() { logging.SetLogPath(previousLogPath) })
			if err := repo.EnsureFileChunks(&file, nil); err == nil || strings.HasPrefix(err.Error(), "resource size mismatch:") {
				t.Fatalf("unauthenticated chunks reached size comparison: %v", err)
			}
			if _, err := os.Stat(logPath); !os.IsNotExist(err) {
				t.Fatalf("unauthenticated chunks produced size diagnostics: %v", err)
			}
		})
	}
}
