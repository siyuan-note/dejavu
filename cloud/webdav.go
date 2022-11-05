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
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
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
}

func (webdav *WebDAV) CreateRepo(name string) (err error) {
	userId := webdav.Conf.UserID

	key := path.Join("siyuan", userId, "repo", name, ".dejavu")
	err = webdav.Client.Write(key, []byte(""), 0644)
	return
}

func (webdav *WebDAV) RemoveRepo(name string) (err error) {
	if !IsValidCloudDirName(name) {
		err = errors.New("invalid repo name")
		return
	}

	userId := webdav.Conf.UserID
	key := path.Join("siyuan", userId, "repo", name)
	err = webdav.Client.RemoveAll(key)
	err = webdav.parseErr(err)
	return
}

func (webdav *WebDAV) GetRepos() (repos []*Repo, size int64, err error) {
	userId := webdav.Conf.UserID

	key := path.Join("siyuan", userId, "repo")
	entries, err := webdav.Client.ReadDir(key)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		repo := &Repo{
			Name:    entry.Name(),
			Size:    entry.Size(), // FIXME: 计算实际大小，而不是文件夹大小
			Updated: entry.ModTime().Format("2006-01-02 15:04:05"),
		}
		repos = append(repos, repo)
	}

	if 1 > len(repos) {
		repos = []*Repo{}
	}
	sort.Slice(repos, func(i, j int) bool { return repos[i].Name < repos[j].Name })

	for _, repo := range repos {
		size += repo.Size
	}
	return
}

func (webdav *WebDAV) GetAvailableSize() (size int64) {
	return 1024 * 1024 * 1024 * 1024 * 2 // 2TB
}

func (webdav *WebDAV) UploadObject(filePath string, overwrite bool) (err error) {
	absFilePath := filepath.Join(webdav.Conf.RepoPath, filePath)
	data, err := os.ReadFile(absFilePath)
	if nil != err {
		return
	}

	key := path.Join("siyuan", webdav.Conf.UserID, "repo", webdav.Conf.Dir, filePath)
	folder := path.Dir(key)
	err = webdav.Client.MkdirAll(folder, 0755)
	if nil != err {
		err = webdav.parseErr(err)
		return
	}

	err = webdav.Client.Write(key, data, 0644)
	err = webdav.parseErr(err)
	return
}

func (webdav *WebDAV) DownloadObject(filePath string) (data []byte, err error) {
	data, err = webdav.Client.Read(filePath)
	err = webdav.parseErr(err)
	return
}

func (webdav *WebDAV) RemoveObject(key string) (err error) {
	userId := webdav.Conf.UserID
	dir := webdav.Conf.Dir

	if !strings.HasPrefix(key, path.Join("siyuan", userId, "repo", dir, "refs", "tags")) { // 仅允许删除标签
		err = errors.New("invalid key")
		return
	}

	err = webdav.Client.Remove(key)
	err = webdav.parseErr(err)
	return
}

func (webdav *WebDAV) GetTags() (tags []*Ref, err error) {
	userId := webdav.Conf.UserID
	dir := webdav.Conf.Dir

	tags, err = webdav.listRepoRefs(userId, dir, "tags")
	if nil != err {
		err = webdav.parseErr(err)
		return
	}
	if 1 > len(tags) {
		tags = []*Ref{}
	}
	return
}

func (webdav *WebDAV) GetRefsFiles() (fileIDs []string, err error) {
	userId := webdav.Conf.UserID
	dir := webdav.Conf.Dir

	refs, err := webdav.listRepoRefs(userId, dir, "")
	repoKey := path.Join("siyuan", userId, "repo", dir)
	var files []string
	for _, ref := range refs {
		index, getErr := webdav.repoIndex(repoKey, ref.ID)
		if nil != getErr {
			return
		}
		if nil == index {
			continue
		}

		files = append(files, index.Files...)
	}
	files = gulu.Str.RemoveDuplicatedElem(files)
	if 1 > len(files) {
		files = []string{}
	}
	return
}

func (webdav *WebDAV) GetChunks(checkChunkIDs []string) (chunkIDs []string, err error) {
	userId := webdav.Conf.UserID
	dir := webdav.Conf.Dir

	repoKey := path.Join("siyuan", userId, "repo", dir)
	var keys []string
	for _, chunk := range checkChunkIDs {
		key := path.Join(repoKey, "objects", chunk[:2], chunk[2:])
		keys = append(keys, key)
	}

	notFound, err := webdav.getNotFound(keys)
	if nil != err {
		return
	}
	chunkIDs = append(chunkIDs, notFound...)
	chunkIDs = gulu.Str.RemoveDuplicatedElem(chunkIDs)
	if 1 > len(chunkIDs) {
		chunkIDs = []string{}
	}
	return
}

func (webdav *WebDAV) GetStat() (stat *Stat, err error) {
	userId := webdav.Conf.UserID

	syncSize, backupSize, syncFileCount, backupCount, backupFileCount, repoCount, syncUpdated, backupUpdated, err := webdav.repoStat(userId)
	if nil != err {
		return
	}

	stat = &Stat{
		Sync: &StatSync{
			Size:      syncSize,
			FileCount: syncFileCount,
			Updated:   syncUpdated,
		},
		Backup: &StatBackup{
			Size:      backupSize,
			Count:     backupCount,
			FileCount: backupFileCount,
			Updated:   backupUpdated,
		},
		AssetSize: 0, // 不统计图床资源大小
		RepoCount: repoCount,
	}
	return
}

func (webdav *WebDAV) repoStat(userId string) (syncSize, backupSize int64, syncFileCount, backupCount, backupFileCount, repoCount int, syncUpdated, backupUpdated string, err error) {
	repos, err := webdav.listRepos(userId)
	if nil != err {
		return
	}
	repoCount = len(repos)

	for _, repo := range repos {
		var refs []*Ref
		refs, err = webdav.listRepoRefs(userId, repo.Name, "")
		if nil != err {
			logging.LogErrorf("list repo refs for user [%s] failed: %s", userId, err)
			return
		}

		repoKey := path.Join("siyuan", userId, "repo", repo.Name)
		for _, ref := range refs {
			index, getErr := webdav.repoIndex(repoKey, ref.ID)
			if nil != getErr {
				err = getErr
				return
			}
			if nil == index {
				continue
			}

			if "latest" == ref.Name {
				syncSize += index.Size
				syncFileCount += index.Count
				if syncUpdated < repo.Updated {
					syncUpdated = ref.Updated
				}
			} else {
				if backupSize < index.Size {
					backupSize = index.Size
				}
				backupCount++
				backupFileCount += index.Count
				if backupUpdated < ref.Updated {
					backupUpdated = ref.Updated
				}
			}
		}
	}
	return
}

func (webdav *WebDAV) listRepoRefs(userId, repo, refPrefix string) (ret []*Ref, err error) {
	keyPath := path.Join("siyuan", userId, "repo", repo, "refs", refPrefix)
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
			Updated: info.ModTime().Format("2006-01-02 15:04:05"),
		}
		ret = append(ret, ref)
	}
	return
}

func (webdav *WebDAV) listRepos(userId string) (ret []*Repo, err error) {
	infos, err := webdav.Client.ReadDir(path.Join("siyuan", userId, "repo"))
	if nil != err {
		err = webdav.parseErr(err)
		return
	}

	for _, repoInfo := range infos {
		dir := repoInfo.Name()
		repo := path.Join("siyuan", userId, "repo", dir)
		latestID, getErr := webdav.repoLatest(repo)
		if nil != getErr {
			return nil, getErr
		}
		var size int64
		var updated string
		if "" != latestID {
			var latest *entity.Index
			latest, getErr = webdav.repoIndex(repo, latestID)
			if nil != getErr {
				return nil, getErr
			}
			if nil == latest {
				continue
			}
			size = latest.Size
			updated = time.UnixMilli(latest.Created).Format("2006-01-02 15:04:05")
		} else {
			info, statErr := webdav.Client.Stat(path.Join(repo, ".dejavu"))
			if nil != statErr {
				updated = time.Now().Format("2006-01-02 15:04:05")
			} else {
				updated = info.ModTime().Format("2006-01-02 15:04:05")
			}
		}

		ret = append(ret, &Repo{
			Name:    dir,
			Size:    size,
			Updated: updated,
		})
	}
	return
}

func (webdav *WebDAV) repoLatest(repoDir string) (id string, err error) {
	latestPath := path.Join(repoDir, "refs", "latest")
	_, err = webdav.Client.Stat(latestPath)
	if nil != err {
		// TODO:
		// if isErrNotFound(err) {
		//	err = nil
		//  return
		//}
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
		info, _ := webdav.Client.Stat(key)
		if nil == info {
			ret = append(ret, key)
		}
	}
	return
}

func (webdav *WebDAV) repoIndex(repoDir, id string) (ret *entity.Index, err error) {
	indexPath := path.Join(repoDir, "indexes", id)
	info, err := webdav.Client.Stat(indexPath)
	if nil != err {
		// TODO:
		// if isErrNotFound(err) {
		//	err = nil
		// return
		//}
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
				} else if 503 == statusErr.Status {
					return ErrCloudServiceUnavailable
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
