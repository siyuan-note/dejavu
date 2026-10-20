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
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

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
	dirs map[string]time.Time
}

// ErrWebDAVInvalidResponse 表示端点没有返回 WebDAV 集合或同步元数据。
var ErrWebDAVInvalidResponse = errors.New("WebDAV endpoint did not return a collection or repository data")

func NewWebDAV(baseCloud *BaseCloud, client *gowebdav.Client) (ret *WebDAV) {
	ret = &WebDAV{
		BaseCloud: baseCloud,
		Client:    client,
		lock:      sync.Mutex{},
		dirs:      map[string]time.Time{},
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
	file, err := os.Open(absFilePath)
	if nil != err {
		return
	}
	defer file.Close()

	info, err := file.Stat()
	if nil != err {
		return
	}
	length = info.Size()

	key := path.Join(webdav.Dir, "siyuan", "repo", filePath)
	defer func() {
		if nil != err {
			webdav.clearDirs()
		}
	}()
	folder := path.Dir(key)
	if err = webdav.mkdirAll(folder); nil != err {
		return
	}

	err = webdav.Client.WriteStreamWithLength(key, file, length, 0644)
	err = webdav.parseErr(err)
	if nil != err {
		logging.LogErrorf("upload object [%s] failed: %s", key, err)
		return
	}
	if "refs/latest" == filePath {
		// WebDAV 可能允许写入无扩展名文件，但拒绝随后读取，不能将这种上传报告为成功。
		if _, err = webdav.readMetadata(key); nil != err {
			err = webdav.parseErr(err)
			if errors.Is(err, ErrCloudObjectNotFound) {
				err = fmt.Errorf("read uploaded WebDAV refs/latest: %w", ErrCloudCheckFailed)
			}
		}
	}
	return
}

func (webdav *WebDAV) UploadBytes(filePath string, data []byte, overwrite bool) (length int64, err error) {
	defer func() {
		if nil != err {
			webdav.clearDirs()
		}
	}()
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
	if nil == err && (strings.HasPrefix(filePath, "refs/") || filePath == "lock-sync") && webDAVHTML(data) {
		return nil, ErrWebDAVInvalidResponse
	}
	err = webdav.parseErr(err)
	if nil != err {
		if "refs/latest" == filePath && errors.Is(err, ErrCloudObjectNotFound) {
			// 非空索引清单说明云端已有数据，不能把无法读取的同步指针当作空仓库。
			indexesData, readErr := webdav.Client.Read(path.Join(webdav.Dir, "siyuan", "repo", "indexes-v2.json"))
			if nil != readErr {
				if readErr = webdav.parseErr(readErr); !errors.Is(readErr, ErrCloudObjectNotFound) {
					err = readErr
				}
				return
			}
			indexesData, readErr = compressDecoder.DecodeAll(indexesData, nil)
			if nil != readErr {
				err = readErr
				return
			}
			var indexes *Indexes
			if readErr = gulu.JSON.UnmarshalJSON(indexesData, &indexes); nil != readErr {
				err = readErr
				return
			}
			if nil == indexes || 0 < len(indexes.Indexes) {
				err = fmt.Errorf("missing WebDAV refs/latest with existing cloud indexes: %w", ErrCloudCheckFailed)
			}
		}
		return
	}

	//logging.LogInfof("downloaded object [%s]", key)
	return
}

func (webdav *WebDAV) RemoveObject(filePath string) (err error) {
	defer webdav.clearDirs()
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
		if errors.Is(err, ErrCloudObjectNotFound) { // https://ld246.com/article/1749182255326
			err = nil
			tags = []*Ref{}
			return
		}
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
			err = webdav.parseErr(getErr)
			ret = nil
			return
		}
		if nil == index {
			err = ErrCloudCheckFailed
			ret = nil
			return
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

		data, ReadErr := webdav.readMetadata(path.Join(keyPath, info.Name()))
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

	data, err := webdav.readMetadata(latestPath)
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
					return ErrWebDAVInvalidResponse
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
	webdav.lock.Lock()
	defer webdav.lock.Unlock()
	// 目录缓存只属于当前客户端，限时复核，失败后不再复用已有缓存。
	defer func() {
		if nil != err {
			clear(webdav.dirs)
		}
	}()
	if time.Now().Before(webdav.dirs[path.Clean(folder)]) {
		return nil
	}

	info, err := webdav.Client.Stat(folder)
	if nil == err {
		if !info.IsDir() {
			return &fs.PathError{Op: "Mkdir", Path: folder, Err: fs.ErrExist}
		}
		webdav.dirs[path.Clean(folder)] = time.Now().Add(time.Minute)
		return nil
	}
	if !errors.Is(webdav.parseErr(err), ErrCloudObjectNotFound) {
		return webdav.parseErr(err)
	}

	paths := strings.Split(folder, "/")
	sub := "/"
	for _, e := range paths {
		if e == "" {
			continue
		}
		sub += e + "/"

		if time.Now().Before(webdav.dirs[path.Clean(sub)]) {
			continue
		}

		err = webdav.Client.Mkdir(sub, 0755)
		err = webdav.parseErr(err)
		if nil != err {
			return err
		}
		// gowebdav 将 MKCOL 的 405 当作成功，必须确认目标确实为集合。
		info, err = webdav.Client.Stat(sub)
		if nil != err {
			if errors.Is(webdav.parseErr(err), ErrCloudObjectNotFound) {
				return fmt.Errorf("WebDAV collection not created: %w", ErrWebDAVInvalidResponse)
			}
			return webdav.parseErr(err)
		}
		if !info.IsDir() {
			return &fs.PathError{Op: "Mkdir", Path: sub, Err: fs.ErrExist}
		}
		webdav.dirs[path.Clean(sub)] = time.Now().Add(time.Minute)
	}
	return nil
}

func (webdav *WebDAV) clearDirs() {
	webdav.lock.Lock()
	defer webdav.lock.Unlock()
	clear(webdav.dirs)
}

// 只检查格式明确的明文元数据，不对加密对象按文本前缀进行判定。
func (webdav *WebDAV) readMetadata(key string) ([]byte, error) {
	data, err := webdav.Client.Read(key)
	if nil == err && webDAVHTML(data) {
		return nil, ErrWebDAVInvalidResponse
	}
	return data, err
}

func webDAVHTML(data []byte) bool {
	data = bytes.TrimSpace(bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf}))
	if len(data) > 32 {
		data = data[:32]
	}
	prefix := strings.ToLower(string(data))
	return strings.HasPrefix(prefix, "<!doctype html") || strings.HasPrefix(prefix, "<html")
}
