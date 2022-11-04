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
	err = webdav.Client.Write(key, data, 0644)
	err = webdav.parseErr(err)
	return
}

func (webdav *WebDAV) DownloadObject(filePath string) (data []byte, err error) {
	data, err = webdav.Client.Read(filePath)
	err = webdav.parseErr(err)
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
