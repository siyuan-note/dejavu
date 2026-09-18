// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package dejavu

import (
	"encoding/json"
	"errors"
	"path"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

type testChunkSource struct {
	chunks map[string][]byte
	hasErr error
	getErr error

	concurrentReqs int
	downloadDelay  time.Duration
	active         atomic.Int32
	maxActive      atomic.Int32
}

func (source *testChunkSource) Name() string {
	return "test"
}

func (source *testChunkSource) HasChunks(ids []string) (ret map[string]bool, err error) {
	if nil != source.hasErr {
		return nil, source.hasErr
	}
	ret = map[string]bool{}
	for _, id := range ids {
		_, ret[id] = source.chunks[id]
	}
	return
}

func (source *testChunkSource) DownloadChunk(id string) (data []byte, err error) {
	active := source.active.Add(1)
	defer source.active.Add(-1)
	for current := source.maxActive.Load(); current < active && !source.maxActive.CompareAndSwap(current, active); {
		current = source.maxActive.Load()
	}
	time.Sleep(source.downloadDelay)
	if nil != source.getErr {
		return nil, source.getErr
	}
	data, ok := source.chunks[id]
	if !ok {
		return nil, errors.New("chunk not found")
	}
	return data, nil
}

func (source *testChunkSource) GetConcurrentReqs() int {
	if 0 < source.concurrentReqs {
		return source.concurrentReqs
	}
	return 4
}

func (source *testChunkSource) DownloadChunkValidated(id string, validate func(data []byte) error) (data []byte, err error) {
	data, err = source.DownloadChunk(id)
	if nil == err && nil != validate {
		err = validate(data)
	}
	return
}

func (source *testChunkSource) HasObjects(ids []string) (ret map[string]bool, err error) {
	return source.HasChunks(ids)
}

func (source *testChunkSource) DownloadObjectValidated(id string, validate func(data []byte) error) (data []byte, err error) {
	return source.DownloadChunkValidated(id, validate)
}

func TestDownloadChunksFromSource(t *testing.T) {
	repo, _ := newChunkSourceTestRepo(t)
	data := []byte("peer chunk")
	id := util.Hash(data)
	encoded, err := repo.store.encodeData(data)
	if nil != err {
		t.Fatal(err)
	}
	repo.SetChunkSource(&testChunkSource{chunks: map[string][]byte{id: encoded}})

	stat, err := repo.downloadCloudChunksPut([]string{id}, map[string]interface{}{})
	if nil != err {
		t.Fatal(err)
	}
	if 1 != stat.PeerCount || int64(len(encoded)) != stat.PeerBytes || 0 != stat.CloudBytes || 0 != stat.PeerFallbackCount {
		t.Fatalf("unexpected download stat: %+v", stat)
	}
	chunk, err := repo.store.GetChunk(id)
	if nil != err {
		t.Fatal(err)
	}
	if string(data) != string(chunk.Data) {
		t.Fatalf("unexpected chunk data [%s]", chunk.Data)
	}
}

func TestDownloadFilesFromSource(t *testing.T) {
	repo, _ := newChunkSourceTestRepo(t)
	file := entity.NewFile("data/test.sy", 42, time.Now().UnixMilli())
	file.Chunks = []string{util.Hash([]byte("chunk"))}
	data, err := json.Marshal(file)
	if nil != err {
		t.Fatal(err)
	}
	encoded, err := repo.store.encodeData(data)
	if nil != err {
		t.Fatal(err)
	}
	repo.SetChunkSource(&testChunkSource{chunks: map[string][]byte{file.ID: encoded}})

	stat, files, err := repo.downloadCloudFilesPut([]string{file.ID}, map[string]interface{}{})
	if nil != err {
		t.Fatal(err)
	}
	if 1 != stat.PeerCount || int64(len(encoded)) != stat.PeerBytes || 0 != stat.CloudBytes || 0 != stat.PeerFallbackCount {
		t.Fatalf("unexpected download stat: %+v", stat)
	}
	if 1 != len(files) || file.ID != files[0].ID || file.Path != files[0].Path {
		t.Fatalf("unexpected downloaded files: %+v", files)
	}
	stored, err := repo.store.GetFile(file.ID)
	if nil != err {
		t.Fatal(err)
	}
	if stored.ID != file.ID || stored.Path != file.Path {
		t.Fatalf("unexpected stored file: %+v", stored)
	}
}

func TestDownloadFilesFallsBackToCloud(t *testing.T) {
	repo, localCloud := newChunkSourceTestRepo(t)
	file := entity.NewFile("data/cloud.sy", 42, time.Now().UnixMilli())
	data, err := json.Marshal(file)
	if nil != err {
		t.Fatal(err)
	}
	encoded, err := repo.store.encodeData(data)
	if nil != err {
		t.Fatal(err)
	}
	if _, err = localCloud.UploadBytes(path.Join("objects", file.ID[:2], file.ID[2:]), encoded, false); nil != err {
		t.Fatal(err)
	}
	invalidFile := entity.NewFile("data/invalid.sy", 42, file.Updated)
	invalidData, err := json.Marshal(invalidFile)
	if nil != err {
		t.Fatal(err)
	}
	invalidEncoded, err := repo.store.encodeData(invalidData)
	if nil != err {
		t.Fatal(err)
	}
	repo.SetChunkSource(&testChunkSource{chunks: map[string][]byte{file.ID: invalidEncoded}})

	stat, files, err := repo.downloadCloudFilesPut([]string{file.ID}, map[string]interface{}{})
	if nil != err {
		t.Fatal(err)
	}
	if 0 != stat.PeerCount || 0 != stat.PeerBytes || int64(len(data)) != stat.CloudBytes || 1 != stat.PeerFallbackCount {
		t.Fatalf("unexpected download stat: %+v", stat)
	}
	if 1 != len(files) || file.ID != files[0].ID {
		t.Fatalf("unexpected downloaded files: %+v", files)
	}
}

func TestDownloadChunksFallsBackToCloud(t *testing.T) {
	repo, localCloud := newChunkSourceTestRepo(t)
	data := []byte("cloud chunk")
	id := util.Hash(data)
	encoded, err := repo.store.encodeData(data)
	if nil != err {
		t.Fatal(err)
	}
	if _, err = localCloud.UploadBytes(path.Join("objects", id[:2], id[2:]), encoded, false); nil != err {
		t.Fatal(err)
	}

	invalidData, err := repo.store.encodeData([]byte("invalid peer chunk"))
	if nil != err {
		t.Fatal(err)
	}
	repo.SetChunkSource(&testChunkSource{chunks: map[string][]byte{id: invalidData}})

	stat, err := repo.downloadCloudChunksPut([]string{id}, map[string]interface{}{})
	if nil != err {
		t.Fatal(err)
	}
	if 0 != stat.PeerCount || 0 != stat.PeerBytes || int64(len(data)) != stat.CloudBytes || 1 != stat.PeerFallbackCount {
		t.Fatalf("unexpected download stat: %+v", stat)
	}
	chunk, err := repo.store.GetChunk(id)
	if nil != err {
		t.Fatal(err)
	}
	if string(data) != string(chunk.Data) {
		t.Fatalf("unexpected chunk data [%s]", chunk.Data)
	}
}

func TestDownloadChunksFallsBackWhenSourceQueryFails(t *testing.T) {
	repo, localCloud := newChunkSourceTestRepo(t)
	data := []byte("cloud only chunk")
	id := util.Hash(data)
	encoded, err := repo.store.encodeData(data)
	if nil != err {
		t.Fatal(err)
	}
	if _, err = localCloud.UploadBytes(path.Join("objects", id[:2], id[2:]), encoded, false); nil != err {
		t.Fatal(err)
	}
	repo.SetChunkSource(&testChunkSource{chunks: map[string][]byte{}, hasErr: errors.New("unavailable")})

	stat, err := repo.downloadCloudChunksPut([]string{id}, map[string]interface{}{})
	if nil != err {
		t.Fatal(err)
	}
	if 0 != stat.PeerCount || int64(len(data)) != stat.CloudBytes || 0 != stat.PeerFallbackCount {
		t.Fatalf("unexpected download stat: %+v", stat)
	}
}

func TestDownloadChunksLimitsSourceConcurrency(t *testing.T) {
	repo, _ := newChunkSourceTestRepo(t)
	chunks := map[string][]byte{}
	ids := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		data := []byte{byte(i), byte(i + 1), byte(i + 2)}
		id := util.Hash(data)
		encoded, err := repo.store.encodeData(data)
		if nil != err {
			t.Fatal(err)
		}
		chunks[id] = encoded
		ids = append(ids, id)
	}
	source := &testChunkSource{
		chunks:         chunks,
		concurrentReqs: 2,
		downloadDelay:  50 * time.Millisecond,
	}
	repo.SetChunkSource(source)

	stat, err := repo.downloadCloudChunksPut(ids, map[string]interface{}{})
	if nil != err {
		t.Fatal(err)
	}
	if len(ids) != stat.PeerCount || 2 != source.maxActive.Load() {
		t.Fatalf("unexpected source concurrency or stat: max=%d, stat=%+v", source.maxActive.Load(), stat)
	}
}

func newChunkSourceTestRepo(t *testing.T) (repo *Repo, localCloud *cloud.Local) {
	t.Helper()
	tempDir := t.TempDir()
	repoPath := filepath.Join(tempDir, "repo")
	cloudPath := filepath.Join(tempDir, "cloud")
	localCloud = cloud.NewLocal(&cloud.BaseCloud{Conf: &cloud.Conf{
		Dir:           "main",
		RepoPath:      repoPath,
		AvailableSize: 1024 * 1024 * 1024,
		Local:         &cloud.ConfLocal{Endpoint: cloudPath},
	}})
	var err error
	repo, err = NewRepo(filepath.Join(tempDir, "data"), repoPath, filepath.Join(tempDir, "history"),
		filepath.Join(tempDir, "temp"), "device", "Device", "windows",
		[]byte("0123456789abcdef0123456789abcdef"), nil, localCloud)
	if nil != err {
		t.Fatal(err)
	}
	return
}
