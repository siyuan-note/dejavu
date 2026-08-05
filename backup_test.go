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
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package dejavu

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
)

var errTestIndexUpload = errors.New("index upload failed")

type trackingLocalCloud struct {
	*cloud.Local
	getRefsFilesCalls atomic.Int32
	indexUploads      atomic.Int32
	tagUploads        atomic.Int32
	failIndexUpload   atomic.Bool
}

func (tracking *trackingLocalCloud) GetRefsFiles() (fileIDs []string, refs []*cloud.Ref, err error) {
	tracking.getRefsFilesCalls.Add(1)
	return tracking.Local.GetRefsFiles()
}

func (tracking *trackingLocalCloud) UploadObject(filePath string, overwrite bool) (length int64, err error) {
	if strings.HasPrefix(filePath, "indexes/") {
		tracking.indexUploads.Add(1)
		if tracking.failIndexUpload.Load() {
			return 0, errTestIndexUpload
		}
	}
	if strings.HasPrefix(filePath, "refs/tags/") {
		tracking.tagUploads.Add(1)
	}
	return tracking.Local.UploadObject(filePath, overwrite)
}

func TestUploadTagIndexUsesCloudLatestFastPath(t *testing.T) {
	repo, index, tracking := newUploadTagIndexTestRepo(t)
	if err := repo.AddTag(index.ID, "tag-fast"); nil != err {
		t.Fatal(err)
	}

	uploadFileCount, uploadChunkCount, uploadBytes, err := repo.UploadTagIndex("tag-fast", index.ID, map[string]interface{}{})
	if nil != err {
		t.Fatal(err)
	}
	if 2 != uploadFileCount || 0 != uploadChunkCount || 1 > uploadBytes {
		t.Fatalf("unexpected upload result [files=%d, chunks=%d, bytes=%d]", uploadFileCount, uploadChunkCount, uploadBytes)
	}
	if 0 != tracking.getRefsFilesCalls.Load() {
		t.Fatalf("cloud latest fast path called GetRefsFiles [%d] times", tracking.getRefsFilesCalls.Load())
	}
	if 1 != tracking.indexUploads.Load() || 1 != tracking.tagUploads.Load() {
		t.Fatalf("unexpected uploads [indexes=%d, tags=%d]", tracking.indexUploads.Load(), tracking.tagUploads.Load())
	}
}

func TestUploadTagIndexDoesNotUpdateRefAfterIndexUploadFailure(t *testing.T) {
	repo, index, tracking := newUploadTagIndexTestRepo(t)
	if err := repo.AddTag(index.ID, "tag-fail"); nil != err {
		t.Fatal(err)
	}
	tracking.failIndexUpload.Store(true)

	_, _, _, err := repo.UploadTagIndex("tag-fail", index.ID, map[string]interface{}{})
	if !errors.Is(err, errTestIndexUpload) {
		t.Fatalf("unexpected upload error [%v]", err)
	}
	if 1 != tracking.indexUploads.Load() || 0 != tracking.tagUploads.Load() {
		t.Fatalf("unexpected uploads [indexes=%d, tags=%d]", tracking.indexUploads.Load(), tracking.tagUploads.Load())
	}
}

func newUploadTagIndexTestRepo(t *testing.T) (repo *Repo, index *entity.Index, tracking *trackingLocalCloud) {
	t.Helper()
	tempDir := t.TempDir()
	dataPath := filepath.Join(tempDir, "data")
	repoPath := filepath.Join(tempDir, "repo")
	if err := os.MkdirAll(dataPath, 0755); nil != err {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataPath, "doc.txt"), []byte("data"), 0644); nil != err {
		t.Fatal(err)
	}

	tracking = &trackingLocalCloud{Local: cloud.NewLocal(&cloud.BaseCloud{Conf: &cloud.Conf{
		Dir:           "main",
		RepoPath:      repoPath,
		AvailableSize: 1024 * 1024 * 1024,
		Local:         &cloud.ConfLocal{Endpoint: filepath.Join(tempDir, "cloud")},
	}})}
	var err error
	repo, err = NewRepo(dataPath, repoPath, filepath.Join(tempDir, "history"), filepath.Join(tempDir, "temp"),
		"device", "Device", "windows", []byte("0123456789abcdef0123456789abcdef"), nil, tracking)
	if nil != err {
		t.Fatal(err)
	}
	index, err = repo.Index("Initial index", false, map[string]interface{}{})
	if nil != err {
		t.Fatal(err)
	}
	if _, _, err = repo.Sync(map[string]interface{}{}); nil != err {
		t.Fatal(err)
	}
	tracking.getRefsFilesCalls.Store(0)
	tracking.indexUploads.Store(0)
	tracking.tagUploads.Store(0)
	return
}
