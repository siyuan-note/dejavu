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

package cloud

import (
	"sync"

	"github.com/ricochet2200/go-disk-usage/du"
	"github.com/siyuan-note/dejavu/entity"
)

// Local 描述了本地文件系统服务实现。
type Local struct {
	*BaseCloud

	lock sync.Mutex
}

func NewLocal(baseCloud *BaseCloud) (ret *Local) {
	ret = &Local{
		BaseCloud: baseCloud,
		lock:      sync.Mutex{},
	}
	return
}

func (local *Local) CreateRepo(name string) (err error) {
	// TODO
	err = ErrUnsupported
	return
}

func (local *Local) RemoveRepo(name string) (err error) {
	// TODO
	err = ErrUnsupported
	return
}

func (local *Local) GetRepos() (repos []*Repo, size int64, err error) {
	// TODO
	err = ErrUnsupported
	return
}

func (local *Local) UploadObject(filePath string, overwrite bool) (err error) {
	// TODO
	err = ErrUnsupported
	return
}

func (local *Local) UploadBytes(filePath string, data []byte, overwrite bool) (err error) {
	// TODO
	err = ErrUnsupported
	return
}

func (local *Local) DownloadObject(filePath string) (data []byte, err error) {
	// TODO
	err = ErrUnsupported
	return
}

func (local *Local) RemoveObject(key string) (err error) {
	// TODO
	err = ErrUnsupported
	return
}

func (local *Local) GetTags() (tags []*Ref, err error) {
	// TODO
	err = ErrUnsupported
	return
}

func (local *Local) GetIndexes(page int) (indexes []*entity.Index, pageCount, totalCount int, err error) {
	// TODO
	err = ErrUnsupported
	return
}

func (local *Local) GetRefsFiles() (fileIDs []string, err error) {
	// TODO
	err = ErrUnsupported
	return
}

func (local *Local) GetChunks(checkChunkIDs []string) (chunkIDs []string, err error) {
	// TODO
	err = ErrUnsupported
	return
}

func (local *Local) GetStat() (stat *Stat, err error) {
	// TODO
	err = ErrUnsupported
	return
}

func (local *Local) ListObjects(pathPrefix string) (objInfos map[string]*entity.ObjectInfo, err error) {
	// TODO
	err = ErrUnsupported
	return
}

func (local *Local) GetIndex(id string) (index *entity.Index, err error) {
	// TODO
	err = ErrUnsupported
	return
}

func (local *Local) GetConcurrentReqs() int {
	return local.Local.ConcurrentReqs
}

func (local *Local) GetConf() *Conf {
	return local.Conf
}

func (local *Local) GetAvailableSize() int64 {
	info := du.NewDiskUsage(local.Local.Endpoint)
	return int64(info.Free())
}

func (local *Local) AddTraffic(*Traffic) {
	return
}
