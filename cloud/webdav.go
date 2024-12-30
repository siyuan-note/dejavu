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
	"errors"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/logging"
	"github.com/studio-b12/gowebdav"
)

// WebDAV 描述了 WebDAV 云端存储服务实现。
type WebDAV struct {
	*BaseCloud
	Client *gowebdav.Client

	lock sync.Mutex
}

func NewWebDAV(baseCloud *BaseCloud, client *gowebdav.Client) (ret *WebDAV) {
	ret = &WebDAV{
		BaseCloud: baseCloud,
		Client:    client,
		lock:      sync.Mutex{},
	}
	return
}

func (webdav *WebDAV) GetRepos() (repos []*Repo, size int64, err error) {
	repos, err = webdav.listRepos()
	if nil != err {
		return
	}

	for _, repo := range repos {
		size += repo.Size
	}
	return
}

func (webdav *WebDAV) UploadObject(filePath string, overwrite bool) (length int64, err error) {
	absFilePath := filepath.Join(webdav.Conf.RepoPath, filePath)
	data, err := os.ReadFile(absFilePath)
	if nil != err {
		return
	}

	length, err = webdav.UploadBytes(filePath, data, overwrite)
	return
}

func (webdav *WebDAV) UploadBytes(filePath string, data []byte, overwrite bool) (length int64, err error) {
	length = int64(len(data))
	key := path.Join(webdav.Dir, "siyuan", "repo", filePath)
	folder := path.Dir(key)
	err = webdav.mkdirAll(folder)
	if nil != err {
		return
	}

	err = webdav.Client.Write(key, data, 0644)
	err = webdav.parseErr(err)
	if nil != err {
		logging.LogErrorf("upload object [%s] failed: %s", key, err)
		return
	}
	//logging.LogInfof("uploaded object [%s]", key)
	return
}

func (webdav *WebDAV) DownloadObject(filePath string) (data []byte, err error) {
	key := path.Join(webdav.Dir, "siyuan", "repo", filePath)
	data, err = webdav.Client.Read(key)
	err = webdav.parseErr(err)
	if nil != err {
		return
	}

	//logging.LogInfof("downloaded object [%s]", key)
	return
}

func (webdav *WebDAV) RemoveObject(filePath string) (err error) {
	key := path.Join(webdav.Dir, "siyuan", "repo", filePath)
	err = webdav.Client.Remove(key)
	err = webdav.parseErr(err)
	if nil != err {
		return
	}

	//logging.LogInfof("removed object [%s]", key)
	return
}

func (webdav *WebDAV) GetTags() (tags []*Ref, err error) {
	tags, err = webdav.listRepoRefs("tags")
	if nil != err {
		err = webdav.parseErr(err)
		return
	}
	if 1 > len(tags) {
		tags = []*Ref{}
	}
	return
}

func (webdav *WebDAV) GetIndexes(page int) (ret []*entity.Index, pageCount, totalCount int, err error) {
	ret = []*entity.Index{}
	data, err := webdav.DownloadObject("indexes-v2.json")
	if nil != err {
		err = webdav.parseErr(err)
		if ErrCloudObjectNotFound == err {
			err = nil
		}
		return
	}

	data, err = compressDecoder.DecodeAll(data, nil)
	if nil != err {
		return
	}

	indexesJSON := &Indexes{}
	if err = gulu.JSON.UnmarshalJSON(data, indexesJSON); nil != err {
		return
	}

	totalCount = len(indexesJSON.Indexes)
	pageCount = int(math.Ceil(float64(totalCount) / float64(pageSize)))

	start := (page - 1) * pageSize
	end := page * pageSize
	if end > totalCount {
		end = totalCount
	}

	repoKey := path.Join(webdav.Dir, "siyuan", "repo")
	for i := start; i < end; i++ {
		index, getErr := webdav.repoIndex(repoKey, indexesJSON.Indexes[i].ID)
		if nil != getErr {
			logging.LogWarnf("get index [%s] failed: %s", indexesJSON.Indexes[i], getErr)
			continue
		}

		index.Files = nil // Optimize the performance of obtaining cloud snapshots https://github.com/siyuan-note/siyuan/issues/8387
		ret = append(ret, index)
	}
	return
}

func (webdav *WebDAV) GetRefsFiles() (fileIDs []string, refs []*Ref, err error) {
	refs, err = webdav.listRepoRefs("")
	repoKey := path.Join(webdav.Dir, "siyuan", "repo")
	var files []string
	for _, ref := range refs {
		index, getErr := webdav.repoIndex(repoKey, ref.ID)
		if nil != getErr {
			err = getErr
			return
		}
		if nil == index {
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

func (webdav *WebDAV) GetChunks(checkChunkIDs []string) (chunkIDs []string, err error) {
	repoObjects := path.Join(webdav.Dir, "siyuan", "repo", "objects")
	var keys []string
	for _, chunk := range checkChunkIDs {
		key := path.Join(repoObjects, chunk[:2], chunk[2:])
		keys = append(keys, key)
	}

	notFound, err := webdav.getNotFound(keys)
	if nil != err {
		return
	}

	var notFoundChunkIDs []string
	for _, key := range notFound {
		chunkID := strings.TrimPrefix(key, repoObjects)
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

func (webdav *WebDAV) GetIndex(id string) (index *entity.Index, err error) {
	repoKey := path.Join(webdav.Dir, "siyuan", "repo")
	index, err = webdav.repoIndex(repoKey, id)
	if nil != err {
		logging.LogErrorf("get index [%s] failed: %s", id, err)
		return
	}
	if nil == index {
		err = ErrCloudObjectNotFound
		return
	}
	return
}

func (webdav *WebDAV) GetConcurrentReqs() (ret int) {
	ret = webdav.Conf.WebDAV.ConcurrentReqs
	if 1 > ret {
		ret = 1
	}
	if 16 < ret {
		ret = 16
	}
	return
}

func (webdav *WebDAV) ListObjects(pathPrefix string) (ret map[string]*entity.ObjectInfo, err error) {
	ret = map[string]*entity.ObjectInfo{}

	endWithSlash := strings.HasSuffix(pathPrefix, "/")
	pathPrefix = path.Join(webdav.Dir, "siyuan", "repo", pathPrefix)
	if endWithSlash {
		pathPrefix += "/"
	}

	infos, err := webdav.Client.ReadDir(pathPrefix)
	if nil != err {
		logging.LogErrorf("list objects [%s] failed: %s", pathPrefix, err)
		return
	}

	for _, entry := range infos {
		filePath := entry.Name()
		ret[filePath] = &entity.ObjectInfo{
			Path: filePath,
			Size: entry.Size(),
		}
	}

	if nil != err {
		logging.LogErrorf("list objects failed: %s", err)
		return
	}
	return
}

func (webdav *WebDAV) listRepoRefs(refPrefix string) (ret []*Ref, err error) {
	keyPath := path.Join(webdav.Dir, "siyuan", "repo", "refs", refPrefix)
	infos, err := webdav.Client.ReadDir(keyPath)
	if nil != err {
		err = webdav.parseErr(err)
		return
	}

	for _, info := range infos {
		if info.IsDir() {
			continue
		}

		data, ReadErr := webdav.Client.Read(path.Join(keyPath, info.Name()))
		if nil != ReadErr {
			err = webdav.parseErr(ReadErr)
			return
		}
		id := string(data)
		ref := &Ref{
			Name:    info.Name(),
			ID:      id,
			Updated: info.ModTime().Local().Format("2006-01-02 15:04:05"),
		}
		ret = append(ret, ref)
	}
	return
}

func (webdav *WebDAV) listRepos() (ret []*Repo, err error) {
	infos, err := webdav.Client.ReadDir("/")
	if nil != err {
		err = webdav.parseErr(err)
		if ErrCloudObjectNotFound == err {
			err = nil
		}
		return
	}

	for _, repoInfo := range infos {
		if !repoInfo.IsDir() {
			continue
		}

		ret = append(ret, &Repo{
			Name:    repoInfo.Name(),
			Size:    0,
			Updated: repoInfo.ModTime().Local().Format("2006-01-02 15:04:05"),
		})
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].Name < ret[j].Name })
	return
}

func (webdav *WebDAV) repoLatest(repoDir string) (id string, err error) {
	latestPath := path.Join(repoDir, "refs", "latest")
	_, err = webdav.Client.Stat(latestPath)
	if nil != err {
		err = webdav.parseErr(err)
		return
	}

	data, err := webdav.Client.Read(latestPath)
	if nil != err {
		return
	}
	id = string(data)
	return
}

func (webdav *WebDAV) getNotFound(keys []string) (ret []string, err error) {
	if 1 > len(keys) {
		return
	}
	for _, key := range keys {
		_, statErr := webdav.Client.Stat(key)
		statErr = webdav.parseErr(statErr)
		if ErrCloudObjectNotFound == statErr {
			ret = append(ret, key)
		}
	}
	return
}

func (webdav *WebDAV) repoIndex(repoDir, id string) (ret *entity.Index, err error) {
	indexPath := path.Join(repoDir, "indexes", id)
	info, err := webdav.Client.Stat(indexPath)
	if nil != err {
		err = webdav.parseErr(err)
		return
	}
	if 1 > info.Size() {
		return
	}

	data, err := webdav.Client.Read(indexPath)
	if nil != err {
		return
	}
	data, err = compressDecoder.DecodeAll(data, nil)
	if nil != err {
		return
	}
	ret = &entity.Index{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	return
}

func (webdav *WebDAV) parseErr(err error) error {
	if nil == err {
		return nil
	}

	switch err.(type) {
	case *fs.PathError:
		if e := errors.Unwrap(err); nil != e {
			switch e.(type) {
			case gowebdav.StatusError:
				statusErr := e.(gowebdav.StatusError)
				if 404 == statusErr.Status {
					return ErrCloudObjectNotFound
				} else if 503 == statusErr.Status || 502 == statusErr.Status || 500 == statusErr.Status {
					return ErrCloudServiceUnavailable
				} else if 200 == statusErr.Status {
					return nil
				}
			}
		}
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "404") || strings.Contains(msg, "no such file") {
		err = ErrCloudObjectNotFound
	}
	return err
}

func (webdav *WebDAV) mkdirAll(folder string) (err error) {
	cacheKey := "webdav.dir." + folder
	_, ok := cache.Get(cacheKey)
	if ok {
		return
	}

	webdav.lock.Lock()
	defer webdav.lock.Unlock()

	info, err := webdav.Client.Stat(folder)
	if nil != err {
		err = webdav.parseErr(err)
		if nil == err {
			cache.Set(cacheKey, true, 1)
			return
		}

		if ErrCloudObjectNotFound != err {
			return
		}
	}
	i := info.(*gowebdav.File)
	if nil != i && i.IsDir() {
		cache.Set(cacheKey, true, 1)
		return
	}

	paths := strings.Split(folder, "/")
	sub := "/"
	for _, e := range paths {
		if e == "" {
			continue
		}
		sub += e + "/"

		if _, ok := cache.Get("webdav.dir." + sub); ok {
			continue
		}

		err = webdav.Client.Mkdir(sub, 0755)
		err = webdav.parseErr(err)
		if nil != err {
			logging.LogErrorf("mkdir [%s] failed: %s", folder, err)
		} else {
			cache.Set("webdav.dir."+sub, true, 1)
		}
	}
	return
}
