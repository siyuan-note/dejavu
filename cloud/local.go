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
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/88250/gulu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/logging"
)

// Local 描述了本地文件系统服务实现。
type Local struct {
	*BaseCloud
}

func NewLocal(baseCloud *BaseCloud) (local *Local) {
	local = &Local{
		BaseCloud: baseCloud,
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
	if err != nil {
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
	if err != nil {
		return
	}

	length, err = local.UploadBytes(filePath, data, overwrite)
	return
}

func (local *Local) UploadBytes(filePath string, data []byte, overwrite bool) (length int64, err error) {
	key := path.Join(local.getCurrentRepoDirPath(), filePath)
	folder := path.Dir(key)
	err = os.MkdirAll(folder, 0755)
	if err != nil {
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
	if err != nil {
		logging.LogErrorf("upload object [%s] failed: %s", key, err)
		return
	}

	length = int64(len(data))

	//logging.LogInfof("uploaded object [%s]", key)
	return
}

func (local *Local) DownloadObject(filePath string) (data []byte, err error) {
	key := path.Join(local.getCurrentRepoDirPath(), filePath)
	data, err = os.ReadFile(key)
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrCloudObjectNotFound
		}
		return
	}

	//logging.LogInfof("downloaded object [%s]", key)
	return
}

func (local *Local) RemoveObject(filePath string) (err error) {
	key := path.Join(local.getCurrentRepoDirPath(), filePath)
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

func (local *Local) ListObjects(pathPrefix string) (objects map[string]*entity.ObjectInfo, err error) {
	objects = map[string]*entity.ObjectInfo{}
	pathPrefix = path.Join(local.getCurrentRepoDirPath(), pathPrefix)
	entries, err := os.ReadDir(pathPrefix)
	if err != nil {
		logging.LogErrorf("list objects [%s] failed: %s", pathPrefix, err)
		return
	}

	for _, entry := range entries {
		entryInfo, infoErr := entry.Info()
		if infoErr != nil {
			err = infoErr
			logging.LogErrorf("get object [%s] info failed: %s", path.Join(pathPrefix, entry.Name()), err)
			return
		}

		filePath := entry.Name()
		objects[filePath] = &entity.ObjectInfo{
			Path: filePath,
			Size: entryInfo.Size(),
		}
	}

	//logging.LogInfof("list objects [%s]", pathPrefix)
	return
}

func (local *Local) GetTags() (tags []*Ref, err error) {
	tags, err = local.listRepoRefs("tags")
	if err != nil {
		return
	}
	if 1 > len(tags) {
		tags = []*Ref{}
	}
	return
}

func (local *Local) GetIndexes(page int) (indexes []*entity.Index, pageCount, totalCount int, err error) {
	data, err := local.DownloadObject("indexes-v2.json")
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}

	data, err = compressDecoder.DecodeAll(data, nil)
	if err != nil {
		return
	}

	indexesJSON := &Indexes{}
	if err = gulu.JSON.UnmarshalJSON(data, indexesJSON); err != nil {
		return
	}

	totalCount = len(indexesJSON.Indexes)
	pageCount = int(math.Ceil(float64(totalCount) / float64(pageSize)))
	start := (page - 1) * pageSize
	end := page * pageSize
	if end > totalCount {
		end = totalCount
	}

	for i := start; i < end; i++ {
		index, getErr := local.repoIndex(indexesJSON.Indexes[i].ID)
		if getErr != nil {
			logging.LogWarnf("get repo index [%s] failed: %s", indexesJSON.Indexes[i], getErr)
			continue
		}

		index.Files = nil // Optimize the performance of obtaining cloud snapshots https://github.com/siyuan-note/siyuan/issues/8387
		indexes = append(indexes, index)
	}
	return
}

func (local *Local) GetRefsFiles() (fileIDs []string, refs []*Ref, err error) {
	refs, err = local.listRepoRefs("")
	var files []string
	for _, ref := range refs {
		index, getErr := local.repoIndex(ref.ID)
		if getErr != nil {
			return
		}
		if index == nil {
			continue
		}

		files = append(files, index.Files...)
	}

	fileIDs = gulu.Str.RemoveDuplicatedElem(files)
	if 1 > len(fileIDs) {
		fileIDs = []string{}
	}
	return
}

func (local *Local) GetChunks(checkChunkIDs []string) (chunkIDs []string, err error) {
	repoObjectsPath := path.Join(local.getCurrentRepoDirPath(), "objects")
	var keys []string
	for _, chunkID := range checkChunkIDs {
		key := path.Join(repoObjectsPath, chunkID[:2], chunkID[2:])
		keys = append(keys, key)
	}

	notFound, err := local.getNotFound(keys)
	if err != nil {
		return
	}

	var notFoundChunkIDs []string
	for _, key := range notFound {
		chunkID := strings.TrimPrefix(key, repoObjectsPath)
		chunkID = strings.ReplaceAll(chunkID, "/", "")
		notFoundChunkIDs = append(notFoundChunkIDs, chunkID)
	}

	chunkIDs = append(chunkIDs, notFoundChunkIDs...)
	chunkIDs = gulu.Str.RemoveDuplicatedElem(chunkIDs)
	if 1 > len(chunkIDs) {
		chunkIDs = []string{}
	}
	return
}

// func (local *Local) GetStat() (stat *Stat, err error)

func (local *Local) GetIndex(id string) (index *entity.Index, err error) {
	index, err = local.repoIndex(id)
	if err != nil {
		logging.LogErrorf("get repo index [%s] failed: %s", id, err)
		return
	}
	if index == nil {
		err = ErrCloudObjectNotFound
		return
	}
	return
}

func (local *Local) GetConcurrentReqs() (ret int) {
	ret = local.Local.ConcurrentReqs
	if ret < 1 {
		ret = 16
	}
	if ret > 32 {
		ret = 32
	}
	return
}

func (local *Local) GetConf() *Conf {
	return local.Conf
}

func (local *Local) GetAvailableSize() int64 {
	usage, err := disk.Usage(local.Local.Endpoint)
	if err != nil {
		return math.MaxInt64
	}
	return int64(usage.Free)
}

func (local *Local) AddTraffic(*Traffic) {
	return
}

func (local *Local) listRepos() (repos []*Repo, err error) {
	entries, err := os.ReadDir(local.Local.Endpoint)
	if err != nil {
		logging.LogErrorf("list repos [%s] failed: %s", local.Local.Endpoint, err)
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		entryInfo, infoErr := entry.Info()
		if infoErr != nil {
			err = infoErr
			logging.LogErrorf("get repo [%s] info failed: %s", path.Join(local.Local.Endpoint, entry.Name()), err)
			return
		}
		repos = append(repos, &Repo{
			Name:    entry.Name(),
			Size:    entryInfo.Size(),
			Updated: entryInfo.ModTime().Local().Format("2006-01-02 15:04:05"),
		})
	}
	sort.Slice(repos, func(i, j int) bool { return repos[i].Name < repos[j].Name })
	return
}

func (local *Local) listRepoRefs(refPrefix string) (refs []*Ref, err error) {
	keyPath := path.Join(local.getCurrentRepoDirPath(), "refs", refPrefix)
	entries, err := os.ReadDir(keyPath)
	if err != nil {
		logging.LogErrorf("list repo refs [%s] failed: %s", keyPath, err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		entryInfo, infoErr := entry.Info()
		if infoErr != nil {
			err = infoErr
			logging.LogErrorf("get repo ref [%s] info failed: %s", path.Join(local.Local.Endpoint, entry.Name()), err)
			return
		}

		data, readErr := os.ReadFile(path.Join(keyPath, entry.Name()))
		if readErr != nil {
			err = readErr
			logging.LogErrorf("get repo ref [%s] ID failed: %s", path.Join(local.Local.Endpoint, entry.Name()), err)
			return
		}

		id := string(data)
		ref := &Ref{
			Name:    entry.Name(),
			ID:      id,
			Updated: entryInfo.ModTime().Local().Format("2006-01-02 15:04:05"),
		}
		refs = append(refs, ref)
	}
	return
}

func (local *Local) repoIndex(id string) (index *entity.Index, err error) {
	indexFilePath := path.Join(local.getCurrentRepoDirPath(), "indexes", id)
	indexFileInfo, err := os.Stat(indexFilePath)
	if err != nil {
		return
	}
	if 1 > indexFileInfo.Size() {
		return
	}

	data, err := os.ReadFile(indexFilePath)
	if err != nil {
		return
	}

	data, err = compressDecoder.DecodeAll(data, nil)
	if err != nil {
		return
	}

	index = &entity.Index{}
	err = gulu.JSON.UnmarshalJSON(data, index)
	if err != nil {
		return
	}
	return
}

func (local *Local) getNotFound(keys []string) (ret []string, err error) {
	if 1 > len(keys) {
		return
	}
	for _, key := range keys {
		_, statErr := os.Stat(key)
		if os.IsNotExist(statErr) {
			ret = append(ret, key)
		}
	}
	return
}

func (local *Local) getCurrentRepoDirPath() string {
	return path.Join(local.Local.Endpoint, local.Dir)
}
