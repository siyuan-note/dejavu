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
	"io"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/88250/gulu"
	aoss "github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/logging"
)

// OSS 描述了阿里云 OSS 对象存储服务实现。
type OSS struct {
	*BaseCloud
}

func NewOSS(baseCloud *BaseCloud) *OSS {
	return &OSS{baseCloud}
}

func (oss *OSS) GetRepos() (repos []*Repo, size int64, err error) {
	repos, err = oss.listRepos()
	if nil != err {
		return
	}

	for _, repo := range repos {
		size += repo.Size
	}
	return
}

func (oss *OSS) GetAvailableSize() (size int64) {
	return 1024 * 1024 * 1024 * 1024 * 2 // 2TB
}

func (oss *OSS) UploadObject(filePath string, overwrite bool) (err error) {
	accessKey := oss.Conf.AccessKey
	secretKey := oss.Conf.SecretKey
	endpoint := oss.Conf.Endpoint
	client, err := aoss.New(endpoint, accessKey, secretKey)
	if nil != err {
		return
	}

	bucket, err := client.Bucket(oss.Conf.Bucket)
	if nil != err {
		return
	}

	absFilePath := filepath.Join(oss.Conf.RepoPath, filePath)
	key := path.Join("siyuan", oss.Conf.UserID, "repo", oss.Conf.Dir, filePath)
	err = bucket.PutObjectFromFile(key, absFilePath)
	if nil != err {
		return
	}
	//logging.LogInfof("uploaded object [%s]", key)
	return
}

func (oss *OSS) DownloadObject(key string) (data []byte, err error) {
	accessKey := oss.Conf.AccessKey
	secretKey := oss.Conf.SecretKey
	endpoint := oss.Conf.Endpoint
	client, err := aoss.New(endpoint, accessKey, secretKey)
	if nil != err {
		return
	}

	bucket, err := client.Bucket(oss.Conf.Bucket)
	if nil != err {
		return
	}

	reader, err := bucket.GetObject(key)
	if nil != err {
		if oss.isErrNotFound(err) {
			err = ErrCloudObjectNotFound
		}
		return
	}
	defer reader.Close()

	data, err = io.ReadAll(reader)
	//logging.LogInfof("downloaded object [%s]", key)
	return
}

func (oss *OSS) RemoveObject(key string) (err error) {
	accessKey := oss.Conf.AccessKey
	secretKey := oss.Conf.SecretKey
	endpoint := oss.Conf.Endpoint
	client, err := aoss.New(endpoint, accessKey, secretKey)
	if nil != err {
		return
	}

	bucket, err := client.Bucket(oss.Conf.Bucket)
	if nil != err {
		return
	}

	err = bucket.DeleteObject(key)
	return
}

func (oss *OSS) GetTags() (tags []*Ref, err error) {
	userId := oss.Conf.UserID
	dir := oss.Conf.Dir

	tags, err = oss.listRepoRefs(userId, dir, "tags")
	if nil != err {
		logging.LogErrorf("list repo tags for user [%s] failed: %s", userId, err)
		return
	}
	if 1 > len(tags) {
		tags = []*Ref{}
	}
	return
}

func (oss *OSS) GetRefsFiles() (fileIDs []string, err error) {
	userId := oss.Conf.UserID
	dir := oss.Conf.Dir

	refs, err := oss.listRepoRefs(userId, dir, "")
	if nil != err {
		logging.LogErrorf("list repo refs for user [%s] failed: %s", userId, err)
		return
	}

	repoKey := path.Join("siyuan", userId, "repo", dir)
	var files []string
	for _, ref := range refs {
		index, getErr := oss.repoIndex(repoKey, ref.ID)
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

func (oss *OSS) GetChunks(checkChunkIDs []string) (chunkIDs []string, err error) {
	userId := oss.Conf.UserID
	dir := oss.Conf.Dir

	repoKey := path.Join("siyuan", userId, "repo", dir)
	var keys []string
	for _, chunk := range checkChunkIDs {
		key := path.Join(repoKey, "objects", chunk[:2], chunk[2:])
		keys = append(keys, key)
	}

	notFound, err := oss.getNotFound(keys)
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

func (oss *OSS) GetStat() (stat *Stat, err error) {
	userId := oss.Conf.UserID

	syncSize, backupSize, syncFileCount, backupCount, backupFileCount, repoCount, syncUpdated, backupUpdated, err := oss.repoStat(userId)
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

func (oss *OSS) repoStat(userId string) (syncSize, backupSize int64, syncFileCount, backupCount, backupFileCount, repoCount int, syncUpdated, backupUpdated string, err error) {
	repos, err := oss.listRepos()
	if nil != err {
		return
	}
	repoCount = len(repos)

	for _, repo := range repos {
		var refs []*Ref
		refs, err = oss.listRepoRefs(userId, repo.Name, "")
		if nil != err {
			logging.LogErrorf("list repo refs for user [%s] failed: %s", userId, err)
			return
		}

		repoKey := path.Join("siyuan", userId, "repo", repo.Name)
		for _, ref := range refs {
			index, getErr := oss.repoIndex(repoKey, ref.ID)
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

func (oss *OSS) repoIndex(repoDir, id string) (ret *entity.Index, err error) {
	indexPath := path.Join(repoDir, "indexes", id)
	info, err := oss.statFile(indexPath)
	if nil != err {
		if oss.isErrNotFound(err) {
			err = nil
		}
		return
	}
	if 1 > info.Size {
		return
	}

	data, err := oss.DownloadObject(indexPath)
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

func (oss *OSS) listRepoRefs(userId, repo, refPrefix string) (ret []*Ref, err error) {
	accessKey := oss.Conf.AccessKey
	secretKey := oss.Conf.SecretKey
	endpoint := oss.Conf.Endpoint
	client, err := aoss.New(endpoint, accessKey, secretKey)
	if nil != err {
		return
	}

	bucket, err := client.Bucket(oss.Conf.Bucket)
	if nil != err {
		return
	}
	limit := 32
	prefix := aoss.Prefix(path.Join("siyuan", userId, "repo", repo, "refs", refPrefix))
	continuationToken := aoss.ContinuationToken("")
	for {
		listObjectResult, listErr := bucket.ListObjectsV2(aoss.MaxKeys(limit), continuationToken, prefix)
		if nil != listErr {
			logging.LogErrorf("list failed: %s", listErr)
			return nil, listErr
		}

		prefix = aoss.Prefix(listObjectResult.Prefix)
		continuationToken = aoss.Marker(listObjectResult.NextContinuationToken)

		for _, entry := range listObjectResult.Objects {
			data, getErr := oss.DownloadObject(entry.Key)
			if nil != getErr {
				err = getErr
				return
			}

			id := string(data)
			info, statErr := oss.statFile(path.Join("siyuan", userId, "repo", repo, "indexes", id))
			if nil != statErr {
				err = statErr
				return
			}
			if 1 > info.Size {
				continue
			}

			ret = append(ret, &Ref{
				Name:    path.Base(entry.Key),
				ID:      id,
				Updated: entry.LastModified.Format("2006-01-02 15:04:05"),
			})
		}

		if !listObjectResult.IsTruncated {
			break
		}
	}
	return
}

func (oss *OSS) listRepos() (ret []*Repo, err error) {
	accessKey := oss.Conf.AccessKey
	secretKey := oss.Conf.SecretKey
	endpoint := oss.Conf.Endpoint

	client, err := aoss.New(endpoint, accessKey, secretKey)
	if nil != err {
		return
	}

	listBucketsResult, err := client.ListBuckets()
	if nil != err {
		return
	}

	ret = []*Repo{}
	for _, bucket := range listBucketsResult.Buckets {
		statResult, statErr := client.GetBucketStat(bucket.Name)
		if nil != statErr {
			return
		}

		ret = append(ret, &Repo{
			Name:    bucket.Name,
			Size:    statResult.Storage,
			Updated: time.Unix(statResult.LastModifiedTime, 0).Format("2006-01-02 15:04:05"),
		})
	}

	sort.Slice(ret, func(i, j int) bool { return ret[i].Name < ret[j].Name })
	return
}

func (oss *OSS) repoLatest(repoDir string) (id string, err error) {
	latestPath := path.Join(repoDir, "refs", "latest")
	info, err := oss.statFile(latestPath)
	if nil != err {
		if oss.isErrNotFound(err) {
			err = nil
		}
		return
	}
	if 1 > info.Size {
		// 不存在任何索引
		return
	}

	data, err := oss.DownloadObject(latestPath)
	if nil != err {
		return
	}
	id = string(data)
	return
}

func (oss *OSS) statFile(key string) (info *objectInfo, err error) {
	accessKey := oss.Conf.AccessKey
	secretKey := oss.Conf.SecretKey
	endpoint := oss.Conf.Endpoint

	client, err := aoss.New(endpoint, accessKey, secretKey)
	if nil != err {
		return
	}

	bucket, err := client.Bucket(oss.Conf.Bucket)
	if nil != err {
		return
	}

	header, err := bucket.GetObjectMeta(key)
	if nil != err {
		return
	}

	modified := header.Get("Last-Modified")
	t, _ := time.Parse(time.RFC1123, modified)
	updated := t.Format("2006-01-02 15:04:05")
	contentLen := header.Get("Content-Length")
	size, _ := strconv.ParseInt(contentLen, 10, 64)
	info = &objectInfo{
		Key:     key,
		Updated: updated,
		Size:    size,
	}
	return
}

func (oss *OSS) getNotFound(keys []string) (ret []string, err error) {
	if 1 > len(keys) {
		return
	}
	for _, key := range keys {
		info, statErr := oss.statFile(key)
		if nil == info || nil != statErr {
			ret = append(ret, key)
		}
	}
	return
}

func (oss *OSS) isErrNotFound(err error) bool {
	switch err.(type) {
	case *aoss.ServiceError:
		code := err.(*aoss.ServiceError).StatusCode
		if 404 == code {
			return true
		}
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") || strings.Contains(msg, "404") || strings.Contains(msg, "no such file or directory")
}
