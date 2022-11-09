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
	"context"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/88250/gulu"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"github.com/qiniu/go-sdk/v7/auth"
	"github.com/qiniu/go-sdk/v7/client"
	"github.com/qiniu/go-sdk/v7/storage"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/httpclient"
	"github.com/siyuan-note/logging"
)

// Qiniu 描述了七牛云对象存储服务实现。
type Qiniu struct {
	*BaseCloud
}

func NewQiniu(baseCloud *BaseCloud) *Qiniu {
	return &Qiniu{baseCloud}
}

func (qiniu *Qiniu) GetRepos() (repos []*Repo, size int64, err error) {
	userId := qiniu.Conf.UserID

	repos, err = qiniu.listRepos()
	if nil != err {
		logging.LogErrorf("list repos for user [%s] failed: %s", userId, err)
		return
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

func (qiniu *Qiniu) UploadObject(filePath string, overwrite bool) (err error) {
	absFilePath := filepath.Join(qiniu.Conf.RepoPath, filePath)

	key := path.Join("siyuan", qiniu.Conf.UserID, "repo", qiniu.Conf.Dir, filePath)
	keyUploadToken, scopeUploadToken, err := qiniu.getScopeKeyUploadToken(key)
	if nil != err {
		return
	}

	uploadToken := keyUploadToken
	if !overwrite {
		uploadToken = scopeUploadToken
	}

	formUploader := storage.NewFormUploader(nil)
	ret := storage.PutRet{}
	err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, absFilePath, nil)
	if nil != err {
		if e, ok := err.(*client.ErrorInfo); ok && 614 == e.Code {
			// file exists
			//logging.LogWarnf("upload object [%s] exists: %s", absFilePath, err)
			err = nil
			return
		}
		time.Sleep(3 * time.Second)
		err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, absFilePath, nil)
		if nil != err {
			logging.LogErrorf("upload object [%s] failed: %s", absFilePath, err)
			return
		}
	}
	//logging.LogInfof("uploaded object [%s]", key)
	return
}

func (qiniu *Qiniu) DownloadObject(key string) (data []byte, err error) {
	endpoint := qiniu.Conf.Endpoint
	url := endpoint + key

	resp, err := httpclient.NewCloudFileRequest15s().Get(url)
	if nil != err {
		err = fmt.Errorf("download object [%s] failed: %s", key, err)
		return
	}
	if 200 != resp.StatusCode {
		if 404 == resp.StatusCode {
			if !strings.HasSuffix(key, "/refs/latest") {
				logging.LogErrorf("download object [%s] failed: %s", key, ErrCloudObjectNotFound)
			}
			err = ErrCloudObjectNotFound
			return
		}
		err = fmt.Errorf("download object [%s] failed [%d]", key, resp.StatusCode)
		return
	}

	data, err = resp.ToBytes()
	if nil != err {
		err = fmt.Errorf("download read data failed: %s", err)
		return
	}
	//logging.LogInfof("downloaded object [%s]", key)
	return
}

func (qiniu *Qiniu) RemoveObject(key string) (err error) {
	userId := qiniu.Conf.UserID
	dir := qiniu.Conf.Dir

	if !strings.HasPrefix(key, path.Join("siyuan", userId, "repo", dir, "refs", "tags")) { // 仅允许删除标签
		err = errors.New("invalid key")
		return
	}

	accessKey := qiniu.Conf.AccessKey
	secretKey := qiniu.Conf.SecretKey
	bucket := qiniu.Conf.Bucket

	mac := auth.New(accessKey, secretKey)
	bucketManager := storage.NewBucketManager(mac, nil)
	err = bucketManager.Delete(bucket, key)
	if nil != err {
		logging.LogErrorf("remove file [%s] failed: %s", key, err)
		return
	}
	return
}

func (qiniu *Qiniu) GetTags() (tags []*Ref, err error) {
	userId := qiniu.Conf.UserID
	dir := qiniu.Conf.Dir

	tags, err = qiniu.listRepoRefs(userId, dir, "tags")
	if nil != err {
		logging.LogErrorf("list repo tags for user [%s] failed: %s", userId, err)
		return
	}
	if 1 > len(tags) {
		tags = []*Ref{}
	}
	return
}

func (qiniu *Qiniu) GetRefsFiles() (fileIDs []string, err error) {
	userId := qiniu.Conf.UserID
	dir := qiniu.Conf.Dir

	refs, err := qiniu.listRepoRefs(userId, dir, "")
	if nil != err {
		logging.LogErrorf("list repo refs for user [%s] failed: %s", userId, err)
		return
	}

	repoKey := path.Join("siyuan", userId, "repo", dir)
	var files []string
	for _, ref := range refs {
		index, getErr := qiniu.repoIndex(repoKey, ref.ID)
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

func (qiniu *Qiniu) GetChunks(checkChunkIDs []string) (chunkIDs []string, err error) {
	userId := qiniu.Conf.UserID
	dir := qiniu.Conf.Dir

	repoKey := path.Join("siyuan", userId, "repo", dir)
	var keys []string
	for _, chunk := range checkChunkIDs {
		key := path.Join(repoKey, "objects", chunk[:2], chunk[2:])
		keys = append(keys, key)
	}

	notFound, err := qiniu.getNotFound(keys)
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

func (qiniu *Qiniu) GetStat() (stat *Stat, err error) {
	userId := qiniu.Conf.UserID

	syncSize, backupSize, syncFileCount, backupCount, backupFileCount, repoCount, syncUpdated, backupUpdated, err := qiniu.repoStat(userId)
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

func (qiniu *Qiniu) repoStat(userId string) (syncSize, backupSize int64, syncFileCount, backupCount, backupFileCount, repoCount int, syncUpdated, backupUpdated string, err error) {
	repos, err := qiniu.listRepos()
	if nil != err {
		return
	}
	repoCount = len(repos)

	for _, repo := range repos {
		var refs []*Ref
		refs, err = qiniu.listRepoRefs(userId, repo.Name, "")
		if nil != err {
			logging.LogErrorf("list repo refs for user [%s] failed: %s", userId, err)
			return
		}

		repoKey := path.Join("siyuan", userId, "repo", repo.Name)
		for _, ref := range refs {
			index, getErr := qiniu.repoIndex(repoKey, ref.ID)
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

func (qiniu *Qiniu) getScopeKeyUploadToken(key string) (keyUploadToken, scopeUploadToken string, err error) {
	userId := qiniu.Conf.UserID
	if !strings.HasPrefix(key, path.Join("siyuan", userId)) {
		err = errors.New("invalid key")
		return
	}
	keyPrefix := path.Join("siyuan", userId)

	accessKey := qiniu.Conf.AccessKey
	secretKey := qiniu.Conf.SecretKey
	bucket := qiniu.Conf.Bucket

	expires := uint64(time.Now().Add(24 * time.Hour).Unix())
	putPolicy := storage.PutPolicy{
		Scope:           bucket + ":" + keyPrefix,
		IsPrefixalScope: 1,
		Expires:         expires}
	mac := auth.New(accessKey, secretKey)
	scopeUploadToken = putPolicy.UploadToken(mac)
	putPolicy = storage.PutPolicy{
		Scope:           bucket + ":" + key,
		IsPrefixalScope: 0,
		Expires:         expires}
	keyUploadToken = putPolicy.UploadToken(mac)
	return
}

func (qiniu *Qiniu) repoIndex(repoDir, id string) (ret *entity.Index, err error) {
	indexPath := path.Join(repoDir, "indexes", id)
	info, err := qiniu.statFile(indexPath)
	if nil != err {
		if qiniu.isErrNotFound(err) {
			err = nil
		}
		return
	}
	if 1 > info.Fsize {
		return
	}

	data, err := qiniu.DownloadObject(indexPath)
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

func (qiniu *Qiniu) listRepoRefs(userId, repo, refPrefix string) (ret []*Ref, err error) {
	accessKey := qiniu.Conf.AccessKey
	secretKey := qiniu.Conf.SecretKey
	bucket := qiniu.Conf.Bucket

	mac := auth.New(accessKey, secretKey)
	bucketManager := storage.NewBucketManager(mac, nil)

	limit := 32
	delimiter := ""
	prefix := path.Join("siyuan", userId, "repo", repo, "refs", refPrefix)
	marker := ""
	for {
		entries, _, nextMarker, hashNext, listErr := bucketManager.ListFiles(bucket, prefix, delimiter, marker, limit)
		if nil != listErr {
			logging.LogErrorf("list failed: %s", listErr)
			return nil, listErr
		}
		for _, entry := range entries {
			data, getErr := qiniu.DownloadObject(entry.Key)
			if nil != getErr {
				err = getErr
				return
			}

			id := string(data)
			info, statErr := qiniu.statFile(path.Join("siyuan", userId, "repo", repo, "indexes", id))
			if nil != statErr {
				err = statErr
				return
			}
			if 1 > info.Fsize {
				continue
			}

			ret = append(ret, &Ref{
				Name:    path.Base(entry.Key),
				ID:      id,
				Updated: storage.ParsePutTime(entry.PutTime).Format("2006-01-02 15:04:05"),
			})
		}
		if !hashNext {
			break
		}
		marker = nextMarker
	}
	return
}

func (qiniu *Qiniu) listRepos() (ret []*Repo, err error) {
	accessKey := qiniu.Conf.AccessKey
	secretKey := qiniu.Conf.SecretKey

	ret = []*Repo{}
	mac := auth.New(accessKey, secretKey)
	bucketManager := storage.NewBucketManager(mac, nil)

	bucketNames, err := bucketManager.Buckets(true)
	if nil != err {
		return
	}

	ret = []*Repo{}
	for _, bucketName := range bucketNames {
		ret = append(ret, &Repo{
			Name: bucketName,
			Size: 0,
		})
	}
	return
}

func (qiniu *Qiniu) repoLatest(repoDir string) (id string, err error) {
	latestPath := path.Join(repoDir, "refs", "latest")
	info, err := qiniu.statFile(latestPath)
	if nil != err {
		if qiniu.isErrNotFound(err) {
			err = nil
		}
		return
	}
	if 1 > info.Fsize {
		// 不存在任何索引
		return
	}

	data, err := qiniu.DownloadObject(latestPath)
	if nil != err {
		return
	}
	id = string(data)
	return
}

func (qiniu *Qiniu) statFile(key string) (info storage.FileInfo, err error) {
	accessKey := qiniu.Conf.AccessKey
	secretKey := qiniu.Conf.SecretKey
	bucket := qiniu.Conf.Bucket

	mac := auth.New(accessKey, secretKey)
	bucketManager := storage.NewBucketManager(mac, nil)
	info, err = bucketManager.Stat(bucket, key)
	return
}

func (qiniu *Qiniu) getNotFound(keys []string) (ret []string, err error) {
	if 1 > len(keys) {
		return
	}

	accessKey := qiniu.Conf.AccessKey
	secretKey := qiniu.Conf.SecretKey
	bucket := qiniu.Conf.Bucket

	mac := auth.New(accessKey, secretKey)
	bucketManager := storage.NewBucketManager(mac, nil)

	var statOps [][]string
	j := -1
	for i, key := range keys {
		if 0 == i%1000 {
			statOps = append(statOps, []string{})
			j++
		}
		statOps[j] = append(statOps[j], storage.URIStat(bucket, key))
	}

	notFoundIndexes := map[int][]int{}
	waitGroup := &sync.WaitGroup{}
	lock := &sync.Mutex{}
	p, _ := ants.NewPoolWithFunc(len(statOps), func(arg interface{}) {
		defer waitGroup.Done()
		if nil != err {
			// 快速失败
			return
		}

		m := arg.(map[string]interface{})
		i := m["i"].(int)
		ops := m["ops"].([]string)
		var batchOpRets []storage.BatchOpRet
		batchOpRets, err = bucketManager.Batch(ops)
		if nil != err {
			logging.LogErrorf("batch stat failed: %s", err)
			return
		}

		for j, batchOpRet := range batchOpRets {
			if 200 == batchOpRet.Code {
				continue
			}

			if 404 == batchOpRet.Code || 612 == batchOpRet.Code || strings.Contains(strings.ToLower(batchOpRet.Data.Error), "no such file or directory") {
				lock.Lock()
				notFoundIndexes[i] = append(notFoundIndexes[i], j)
				lock.Unlock()
			} else {
				logging.LogErrorf("batch stat failed: %s", batchOpRet.Data.Error)
				err = errors.New(batchOpRet.Data.Error)
				return
			}
		}
	})
	for i, ops := range statOps {
		waitGroup.Add(1)
		p.Invoke(map[string]interface{}{
			"i":   i,
			"ops": ops,
		})
	}
	waitGroup.Wait()
	p.Release()
	if nil != err {
		logging.LogErrorf("get not found failed: %s", err)
		return
	}

	for i, indexes := range notFoundIndexes {
		for _, index := range indexes {
			ret = append(ret, keys[i*1000+index])
		}
	}
	return
}

func (qiniu *Qiniu) isErrNotFound(err error) bool {
	switch err.(type) {
	case *storage.ErrorInfo:
		code := err.(*storage.ErrorInfo).Code
		if 404 == code || 612 == code {
			return true
		}
	}
	return strings.Contains(strings.ToLower(err.Error()), "no such file or directory")
}
