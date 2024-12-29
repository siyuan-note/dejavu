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
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/ricochet2200/go-disk-usage/du"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/logging"
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
	repoPath := path.Join(local.Local.Endpoint, name)
	err = os.MkdirAll(repoPath, 0755)
	return
}

func (local *Local) RemoveRepo(name string) (err error) {
	repoPath := path.Join(local.Local.Endpoint, name)
	err = os.RemoveAll(repoPath)
	return
}

func (local *Local) GetRepos() (repos []*Repo, size int64, err error) {
	repos, err = local.listRepos()
	if nil != err {
		return
	}

	for _, repo := range repos {
		size += repo.Size
	}
	return
}

func (local *Local) UploadObject(filePath string, overwrite bool) (length int64, err error) {
	absFilePath := filepath.Join(local.Conf.RepoPath, filePath)
	data, err := os.ReadFile(absFilePath)
	if nil != err {
		return
	}

	length, err = local.UploadBytes(filePath, data, overwrite)
	return
}

func (local *Local) UploadBytes(filePath string, data []byte, overwrite bool) (length int64, err error) {
	length = int64(len(data))

	key := local.getCurrentRepoObjectFilePath(filePath)

	folder := path.Dir(key)
	err = os.MkdirAll(folder, 0755)
	if nil != err {
		return
	}

	if !overwrite { // not overwrite the file
		_, err = os.Stat(key)
		if err != nil {
			if os.IsNotExist(err) { // file not exist
				// do nothing and continue
				err = nil
			} else { // other error
				logging.LogErrorf("upload object [%s] failed: %s", key, err)
				return
			}
		}
	}

	err = os.WriteFile(key, data, 0644)
	if nil != err {
		logging.LogErrorf("upload object [%s] failed: %s", key, err)
		return
	}

	//logging.LogInfof("uploaded object [%s]", key)
	return
}

func (local *Local) DownloadObject(filePath string) (data []byte, err error) {
	key := local.getCurrentRepoObjectFilePath(filePath)

	data, err = os.ReadFile(key)
	if nil != err {
		return
	}

	//logging.LogInfof("downloaded object [%s]", key)
	return
}

func (local *Local) RemoveObject(filePath string) (err error) {
	key := local.getCurrentRepoObjectFilePath(filePath)

	err = os.Remove(key)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		} else {
			logging.LogErrorf("remove object [%s] failed: %s", key, err)
		}
		return
	}

	//logging.LogInfof("removed object [%s]", key)
	return
}

func (local *Local) ListObjects(pathPrefix string) (ret map[string]*entity.ObjectInfo, err error) {
	ret = map[string]*entity.ObjectInfo{}

	endWithSlash := strings.HasSuffix(pathPrefix, "/")
	pathPrefix = local.getCurrentRepoObjectFilePath(pathPrefix)
	if endWithSlash {
		pathPrefix += "/"
	}

	entries, err := os.ReadDir(pathPrefix)
	if err != nil {
		logging.LogErrorf("list objects [%s] failed: %s", pathPrefix, err)
		return
	}

	for _, entry := range entries {
		entryInfo, err := entry.Info()
		if nil != err {
			logging.LogErrorf("get object [%s] info failed: %s", path.Join(pathPrefix, entry.Name()), err)
			continue
		}

		filePath := entry.Name()
		ret[filePath] = &entity.ObjectInfo{
			Path: filePath,
			Size: entryInfo.Size(),
		}
	}

	//logging.LogInfof("list objects [%s]", pathPrefix)
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

func (local *Local) listRepos() (ret []*Repo, err error) {
	entries, err := os.ReadDir(local.Local.Endpoint)
	if err != nil {
		logging.LogErrorf("list repos [%s] failed: %s", local.Local.Endpoint, err)
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		entryInfo, err := entry.Info()
		if nil != err {
			logging.LogErrorf("get repo [%s] info failed: %s", path.Join(local.Local.Endpoint, entry.Name()), err)
			continue
		}
		ret = append(ret, &Repo{
			Name:    entry.Name(),
			Size:    entryInfo.Size(),
			Updated: entryInfo.ModTime().Local().Format("2006-01-02 15:04:05"),
		})
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].Name < ret[j].Name })
	return
}

func (local *Local) getCurrentRepoObjectFilePath(filePath string) string {
	return path.Join(local.Local.Endpoint, local.Dir, "repo", filePath)
}
