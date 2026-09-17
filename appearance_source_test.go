package dejavu

import (
	"encoding/json"
	"errors"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

// appearanceSourceFile 将任意分块固定到归档摘要路径，单独验证传输层的完整内容认证。
func appearanceSourceFile(key string, contents ...string) (*entity.File, map[string][]byte) {
	p := "/storage/appearance-v1" + key + "/" + appearanceArchiveDigest([]byte(strings.Join(contents, ""))) + ".sypkg"
	file := entity.NewFile(p, 0, appearanceEventModified)
	chunks := map[string][]byte{}
	for _, content := range contents {
		data := []byte(content)
		id := util.Hash(data)
		file.Chunks = append(file.Chunks, id)
		file.Size += int64(len(data))
		chunks[id] = data
	}
	return file, chunks
}

func encodeAppearanceSourceFile(t *testing.T, repo *Repo, file *entity.File, chunks map[string][]byte) map[string][]byte {
	t.Helper()
	objects := map[string][]byte{}
	for id, data := range chunks {
		encoded, err := repo.store.encodeData(data)
		if err != nil {
			t.Fatal(err)
		}
		objects[id] = encoded
	}
	data, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	objects[file.ID], err = repo.store.encodeData(data)
	if err != nil {
		t.Fatal(err)
	}
	return objects
}

func TestDownloadAppearanceFilesFromSource(t *testing.T) {
	for _, key := range []string{"/themes/sample", "/icons/sample"} {
		for _, onDemand := range []bool{false, true} {
			t.Run(key+map[bool]string{false: "/full", true: "/ondemand"}[onDemand], func(t *testing.T) {
				repo, _ := newChunkSourceTestRepo(t)
				repo.appearanceSyncEnabled = true
				if err := repo.ConfigureAssetDownloads(onDemand, filepath.Join(t.TempDir(), "assets.json"), "appearance-source"); err != nil {
					t.Fatal(err)
				}
				payload := map[string][]byte{"custom.txt": []byte("complete package content")}
				archive := &appearanceArchive{Key: key, Files: payload, State: appearanceArchiveTestState(t, payload, false)}
				data, err := encodeAppearanceArchive(archive)
				if err != nil {
					t.Fatal(err)
				}
				file, chunks := appearanceSourceFile(key, string(data[:len(data)/2]), string(data[len(data)/2:]))
				objects := encodeAppearanceSourceFile(t, repo, file, chunks)
				source := &testChunkSource{chunks: objects}
				repo.SetChunkSource(source)
				stat, files, err := repo.downloadCloudFilesPut([]string{file.ID}, map[string]interface{}{})
				if err != nil {
					t.Fatal(err)
				}
				wantBytes := int64(len(objects[file.ID]))
				for id := range chunks {
					wantBytes += int64(len(objects[id]))
				}
				if len(files) != 1 || !reflect.DeepEqual(files[0], file) || stat.PeerCount != 1 ||
					stat.PeerFallbackCount != 0 || stat.CloudBytes != 0 || stat.PeerBytes != wantBytes ||
					stat.PrefetchedChunkCount != len(chunks) || file.ID != entity.NewFile(file.Path, file.Size, file.Updated).ID {
					t.Fatalf("unexpected source result: files=%+v stat=%+v", files, stat)
				}
				missing, missingErr := repo.localNotFoundChunks(file.Chunks)
				if missingErr != nil || len(missing) != 0 {
					t.Fatalf("verified source chunks were not retained: %v %v", missing, missingErr)
				}
				opened, openErr := repo.openFile(files[0])
				if openErr != nil || string(opened) != string(data) {
					t.Fatalf("archive bytes were not retained: %v", openErr)
				}
				if _, err = decodeAppearanceArchive(key, archive.Digest, opened); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestDownloadLegacyAppearanceFilesFromSource(t *testing.T) {
	for _, p := range []string{"/themes/sample/theme.css", "/icons/sample/icon.js", "/storage/bazaar/themes/sample.json"} {
		t.Run(p, func(t *testing.T) {
			repo, _ := newChunkSourceTestRepo(t)
			data := []byte("existing ordinary file")
			file := entity.NewFile(p, int64(len(data)), 1700000000123)
			file.Chunks = []string{util.Hash(data)}
			objects := encodeAppearanceSourceFile(t, repo, file, map[string][]byte{file.Chunks[0]: data})
			source := &testChunkSource{chunks: objects}
			repo.SetChunkSource(source)
			stat, files, err := repo.downloadCloudFilesPut([]string{file.ID}, map[string]interface{}{})
			if err != nil || len(files) != 1 || !reflect.DeepEqual(files[0], file) || stat.PeerCount != 1 ||
				stat.PrefetchedChunkCount != 0 || stat.CloudBytes != 0 || stat.PeerBytes != int64(len(objects[file.ID])) {
				t.Fatalf("ordinary legacy file read changed: %+v %+v %v", files, stat, err)
			}
			if _, downloaded := source.downloads.Load(file.Chunks[0]); downloaded {
				t.Fatal("ordinary legacy file unexpectedly prefetched package payload")
			}
		})
	}
}

func TestDownloadAppearanceFilesDisabledPreservesOrdinaryTransport(t *testing.T) {
	for _, marker := range []bool{false, true} {
		t.Run(map[bool]string{false: "archive", true: "marker"}[marker], func(t *testing.T) {
			repo, _ := newChunkSourceTestRepo(t)
			file, _ := appearanceSourceFile("/themes/sample", "opaque archive bytes")
			if marker {
				file = entity.NewFile(appearanceFormatPath, int64(len(appearanceFormatData)), appearanceEventModified)
				file.Chunks = []string{util.Hash([]byte(appearanceFormatData))}
			}
			objects := encodeAppearanceSourceFile(t, repo, file, nil)
			source := &testChunkSource{chunks: objects}
			repo.SetChunkSource(source)
			stat, files, err := repo.downloadCloudFilesPut([]string{file.ID}, nil)
			if err != nil || len(files) != 1 || !reflect.DeepEqual(files[0], file) || stat.PeerCount != 1 ||
				stat.PeerFallbackCount != 0 || stat.PrefetchedChunkCount != 0 || stat.CloudBytes != 0 {
				t.Fatalf("disabled appearance changed ordinary file transport: %+v %+v %v", files, stat, err)
			}
			for _, id := range file.Chunks {
				if _, requested := source.downloads.Load(id); requested {
					t.Fatalf("disabled appearance prefetched an ordinary file chunk: %s", id)
				}
			}
		})
	}
}

func TestDownloadAppearanceMarkerRequiresCloud(t *testing.T) {
	for _, cloudAvailable := range []bool{false, true} {
		t.Run(map[bool]string{false: "unavailable", true: "available"}[cloudAvailable], func(t *testing.T) {
			repo, remote := newChunkSourceTestRepo(t)
			repo.appearanceSyncEnabled = true
			data := []byte(appearanceFormatData)
			file := entity.NewFile(appearanceFormatPath, int64(len(data)), appearanceEventModified)
			file.Chunks = []string{util.Hash(data)}
			objects := encodeAppearanceSourceFile(t, repo, file, map[string][]byte{file.Chunks[0]: data})
			if cloudAvailable {
				for id, encoded := range objects {
					if _, err := remote.UploadBytes(path.Join("objects", id[:2], id[2:]), encoded, false); err != nil {
						t.Fatal(err)
					}
				}
			}
			// 固定旧 ID 无法区分同长度的不同标记；局域网声明不能替代被同步云端的协议归属。
			wrong := strings.Replace(appearanceFormatData, ":1", ":2", 1)
			peerFile := *file
			peerFile.Chunks = []string{util.Hash([]byte(wrong))}
			source := &testChunkSource{chunks: encodeAppearanceSourceFile(t, repo, &peerFile,
				map[string][]byte{peerFile.Chunks[0]: []byte(wrong)})}
			repo.SetChunkSource(source)
			stat, files, err := repo.downloadCloudFilesPut([]string{file.ID}, map[string]interface{}{})
			if stat.PeerCount != 0 || stat.PeerFallbackCount != 1 || stat.PrefetchedChunkCount != 0 {
				t.Fatalf("marker accepted a peer source: %+v", stat)
			}
			if !cloudAvailable {
				if err == nil || len(files) != 0 {
					t.Fatalf("marker did not require authoritative cloud metadata: %+v %v", files, err)
				}
				if _, err = repo.store.Stat(file.ID); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unverified marker was persisted: %v", err)
				}
				return
			}
			if err != nil || len(files) != 1 || !reflect.DeepEqual(files[0], file) || stat.CloudBytes == 0 {
				t.Fatalf("authoritative marker did not replace the peer claim: %+v %+v %v", files, stat, err)
			}
			if err = repo.ensureFileChunks(files[0], map[string]interface{}{}); err != nil {
				t.Fatal(err)
			}
			opened, err := repo.openFile(files[0])
			if err != nil || string(opened) != appearanceFormatData {
				t.Fatalf("incorrect cloud marker content: %s %v", opened, err)
			}
		})
	}
}

func TestDownloadEmptyAppearanceFileFromSource(t *testing.T) {
	repo, _ := newChunkSourceTestRepo(t)
	repo.appearanceSyncEnabled = true
	file, chunks := appearanceSourceFile("/themes/sample", "")
	repo.SetChunkSource(&testChunkSource{chunks: encodeAppearanceSourceFile(t, repo, file, chunks)})
	stat, files, err := repo.downloadCloudFilesPut([]string{file.ID}, map[string]interface{}{})
	if err != nil || len(files) != 1 || stat.PrefetchedChunkCount != 1 || stat.PeerCount != 1 {
		t.Fatalf("empty source file was not verified: %+v %+v %v", files, stat, err)
	}
}

func TestDownloadAppearanceFileSourceRejectsInvalidIdentity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*entity.File, map[string][]byte)
	}{
		{"path", func(file *entity.File, _ map[string][]byte) { file.Path = "/themes/other/theme.css" }},
		{"timestamp", func(file *entity.File, _ map[string][]byte) { file.Updated += 1000 }},
		{"size", func(file *entity.File, _ map[string][]byte) { file.Size++ }},
		{"negative size", func(file *entity.File, _ map[string][]byte) { file.Size = -1 }},
		{"chunk order", func(file *entity.File, _ map[string][]byte) {
			file.Chunks[0], file.Chunks[1] = file.Chunks[1], file.Chunks[0]
		}},
		{"content", func(file *entity.File, chunks map[string][]byte) {
			data := []byte("wrong part")
			id := util.Hash(data)
			chunks[id] = data
			file.Chunks[0] = id
		}},
		{"chunk ID", func(file *entity.File, _ map[string][]byte) { file.Chunks[0] = "../invalid" }},
		{"chunk hash", func(file *entity.File, chunks map[string][]byte) { chunks[file.Chunks[0]] = []byte("wrong part") }},
		{"other path", func(file *entity.File, _ map[string][]byte) { file.Path = "/data/test.sy" }},
		{"private ID", func(file *entity.File, _ map[string][]byte) { file.ID = util.Hash([]byte("unsupported private ID")) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo, _ := newChunkSourceTestRepo(t)
			repo.appearanceSyncEnabled = true
			file, chunks := appearanceSourceFile("/themes/sample", "first part", "second part")
			test.mutate(file, chunks)
			repo.SetChunkSource(&testChunkSource{chunks: encodeAppearanceSourceFile(t, repo, file, chunks)})
			if _, files, err := repo.downloadCloudFilesPut([]string{file.ID}, map[string]interface{}{}); err == nil || len(files) != 0 {
				t.Fatalf("invalid source metadata was accepted: files=%+v err=%v", files, err)
			}
			if _, err := repo.store.Stat(file.ID); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid source metadata was persisted: %v", err)
			}
		})
	}
}

func TestDownloadAppearanceFileSourceFallsBackToCloud(t *testing.T) {
	repo, localCloud := newChunkSourceTestRepo(t)
	repo.appearanceSyncEnabled = true
	file, chunks := appearanceSourceFile("/themes/sample", "valid content")
	cloudObjects := encodeAppearanceSourceFile(t, repo, file, chunks)
	for id, data := range cloudObjects {
		if _, err := localCloud.UploadBytes(path.Join("objects", id[:2], id[2:]), data, false); err != nil {
			t.Fatal(err)
		}
	}
	invalid, wrongChunks := appearanceSourceFile("/themes/sample", "other content")
	invalid.ID, invalid.Path = file.ID, file.Path
	objects := encodeAppearanceSourceFile(t, repo, invalid, wrongChunks)
	repo.SetChunkSource(&testChunkSource{chunks: objects})
	stat, files, err := repo.downloadCloudFilesPut([]string{file.ID}, map[string]interface{}{})
	if err != nil {
		t.Fatal(err)
	}
	if stat.PeerCount != 0 || stat.PeerFallbackCount != 1 || stat.PrefetchedChunkCount != 1 ||
		stat.PeerBytes != int64(len(objects[invalid.Chunks[0]])) || len(files) != 1 || !reflect.DeepEqual(file, files[0]) {
		t.Fatalf("unexpected fallback result: files=%+v stat=%+v", files, stat)
	}
	stored, err := repo.store.GetFile(file.ID)
	if err != nil || !reflect.DeepEqual(stored, file) {
		t.Fatalf("source metadata polluted cloud fallback: %+v %v", stored, err)
	}
	if err = repo.ensureFileChunks(file, map[string]interface{}{}); err != nil {
		t.Fatal(err)
	}
	if err = repo.verifySourceAppearanceFile(file, &chunkDownloadStat{}); err != nil {
		t.Fatal(err)
	}
}

func TestDownloadAppearanceFileSourceReusesSharedChunks(t *testing.T) {
	repo, _ := newChunkSourceTestRepo(t)
	repo.appearanceSyncEnabled = true
	first, chunks := appearanceSourceFile("/themes/first", "shared content")
	second, _ := appearanceSourceFile("/themes/second", "shared content")
	objects := encodeAppearanceSourceFile(t, repo, first, chunks)
	for id, data := range encodeAppearanceSourceFile(t, repo, second, nil) {
		objects[id] = data
	}
	source := &testChunkSource{chunks: objects, concurrentReqs: 2, downloadDelay: 20 * time.Millisecond}
	repo.SetChunkSource(source)
	stat, files, err := repo.downloadCloudFilesPut([]string{first.ID, second.ID}, map[string]interface{}{})
	if err != nil {
		t.Fatal(err)
	}
	wantBytes := int64(len(objects[first.ID]) + len(objects[second.ID]) + len(objects[first.Chunks[0]]))
	if len(files) != 2 || stat.PeerCount != 2 || stat.PrefetchedChunkCount != 1 || stat.PeerBytes != wantBytes {
		t.Fatalf("shared chunk was counted or downloaded repeatedly: %+v", stat)
	}
	requests, _ := source.downloads.Load(first.Chunks[0])
	if requests.(*atomic.Int32).Load() != 1 {
		t.Fatalf("shared chunk was downloaded repeatedly: %d", requests.(*atomic.Int32).Load())
	}
	if source.maxActive.Load() > 2 {
		t.Fatalf("source concurrency limit exceeded: %d", source.maxActive.Load())
	}
	stat, _, err = repo.downloadCloudFilesPut([]string{first.ID}, map[string]interface{}{})
	if err != nil || stat.PrefetchedChunkCount != 0 || stat.PeerBytes != int64(len(objects[first.ID])) {
		t.Fatalf("cached chunks were not reused: %+v %v", stat, err)
	}
}

func TestDownloadAppearanceFileSourceAuthenticatesCachedChunks(t *testing.T) {
	repo, _ := newChunkSourceTestRepo(t)
	repo.appearanceSyncEnabled = true
	file, chunks := appearanceSourceFile("/icons/sample", "valid content")
	if err := repo.store.PutChunk(&entity.Chunk{ID: file.Chunks[0], Data: []byte("other content")}); err != nil {
		t.Fatal(err)
	}
	repo.SetChunkSource(&testChunkSource{chunks: encodeAppearanceSourceFile(t, repo, file, chunks)})
	if _, _, err := repo.downloadCloudFilesPut([]string{file.ID}, map[string]interface{}{}); err == nil {
		t.Fatal("accepted an authenticated object with mismatched chunk identity")
	}
	if _, err := repo.store.Stat(file.ID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid source metadata was persisted: %v", err)
	}
}

func TestDownloadAppearanceFileSourceRejectsUnauthenticatedChunk(t *testing.T) {
	repo, _ := newChunkSourceTestRepo(t)
	repo.appearanceSyncEnabled = true
	file, chunks := appearanceSourceFile("/icons/sample", "valid content")
	objects := encodeAppearanceSourceFile(t, repo, file, chunks)
	objects[file.Chunks[0]][len(objects[file.Chunks[0]])-1] ^= 1
	repo.SetChunkSource(&testChunkSource{chunks: objects})
	if _, _, err := repo.downloadCloudFilesPut([]string{file.ID}, map[string]interface{}{}); err == nil {
		t.Fatal("accepted an unauthenticated appearance chunk")
	}
	for _, id := range []string{file.ID, file.Chunks[0]} {
		if _, err := repo.store.Stat(id); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unverified source object was persisted: %s %v", id, err)
		}
	}
}

func TestDownloadAppearanceFileSourceMissingChunkFallsBack(t *testing.T) {
	repo, localCloud := newChunkSourceTestRepo(t)
	repo.appearanceSyncEnabled = true
	file, chunks := appearanceSourceFile("/icons/sample", "valid content")
	objects := encodeAppearanceSourceFile(t, repo, file, chunks)
	if _, err := localCloud.UploadBytes(path.Join("objects", file.ID[:2], file.ID[2:]), objects[file.ID], false); err != nil {
		t.Fatal(err)
	}
	repo.SetChunkSource(&testChunkSource{chunks: map[string][]byte{file.ID: objects[file.ID]}})
	stat, files, err := repo.downloadCloudFilesPut([]string{file.ID}, map[string]interface{}{})
	if err != nil || len(files) != 1 || stat.PeerCount != 0 || stat.PeerFallbackCount != 1 || stat.PrefetchedChunkCount != 0 {
		t.Fatalf("incomplete source did not fall back safely: %+v %+v %v", files, stat, err)
	}
}

type appearanceAlternativeSource struct {
	*testChunkSource
	candidates [][]byte
	rejected   int
}

func (source *appearanceAlternativeSource) DownloadObjectValidated(_ string, validate func([]byte) error) ([]byte, error) {
	var err error
	for _, data := range source.candidates {
		if err = validate(data); err == nil {
			return data, nil
		}
		source.rejected++
	}
	return nil, err
}

func TestDownloadAppearanceFileSourceTriesNextPeer(t *testing.T) {
	repo, _ := newChunkSourceTestRepo(t)
	repo.appearanceSyncEnabled = true
	file, chunks := appearanceSourceFile("/icons/sample", "valid content")
	objects := encodeAppearanceSourceFile(t, repo, file, chunks)
	invalid, wrongChunks := appearanceSourceFile("/icons/sample", "other content")
	invalid.ID, invalid.Path = file.ID, file.Path
	wrongObjects := encodeAppearanceSourceFile(t, repo, invalid, wrongChunks)
	objects[invalid.Chunks[0]] = wrongObjects[invalid.Chunks[0]]
	source := &appearanceAlternativeSource{
		testChunkSource: &testChunkSource{chunks: objects},
		candidates:      [][]byte{wrongObjects[file.ID], objects[file.ID]},
	}
	repo.SetChunkSource(source)
	stat, files, err := repo.downloadCloudFilesPut([]string{file.ID}, map[string]interface{}{})
	if err != nil || len(files) != 1 || !reflect.DeepEqual(files[0], file) || source.rejected != 1 ||
		stat.PeerCount != 1 || stat.PeerFallbackCount != 0 || stat.PrefetchedChunkCount != 2 || stat.CloudBytes != 0 {
		t.Fatalf("source validation did not retry the next peer: %+v %+v %v", files, stat, err)
	}
}
