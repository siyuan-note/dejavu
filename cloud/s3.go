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
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/88250/gulu"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	as3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/panjf2000/ants/v2"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/logging"
)

// S3 描述了 S3 协议兼容的对象存储服务实现。
type S3 struct {
	*BaseCloud
	HTTPClient *http.Client
}

func NewS3(baseCloud *BaseCloud, httpClient *http.Client) *S3 {
	return &S3{baseCloud, httpClient}
}

func (s3 *S3) GetRepos() (repos []*Repo, size int64, err error) {
	repos, err = s3.listRepos()
	if nil != err {
		return
	}

	for _, repo := range repos {
		size += repo.Size
	}
	return
}

func (s3 *S3) UploadObject(filePath string, overwrite bool) (length int64, err error) {
	svc := s3.getService()
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Duration(s3.S3.Timeout)*time.Second)
	defer cancelFn()

	absFilePath := filepath.Join(s3.Conf.RepoPath, filePath)
	info, err := os.Stat(absFilePath)
	if nil != err {
		logging.LogErrorf("stat failed: %s", err)
		return
	}
	length = info.Size()

	file, err := os.Open(absFilePath)
	if nil != err {
		return
	}
	defer file.Close()
	key := path.Join("repo", filePath)
	_, err = svc.PutObjectWithContext(ctx, &as3.PutObjectInput{
		Bucket:       aws.String(s3.Conf.S3.Bucket),
		Key:          aws.String(key),
		CacheControl: aws.String("no-cache"),
		Body:         file,
	})
	if nil != err {
		return
	}

	logging.LogInfof("uploaded object [%s]", key)
	return
}

func (s3 *S3) DownloadObject(filePath string) (data []byte, err error) {
	svc := s3.getService()
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Duration(s3.S3.Timeout)*time.Second)
	defer cancelFn()
	key := path.Join("repo", filePath)
	input := &as3.GetObjectInput{
		Bucket:               aws.String(s3.Conf.S3.Bucket),
		Key:                  aws.String(key),
		ResponseCacheControl: aws.String("no-cache"),
	}
	resp, err := svc.GetObjectWithContext(ctx, input)
	if nil != err {
		if s3.isErrNotFound(err) {
			err = ErrCloudObjectNotFound
		}
		return
	}
	defer resp.Body.Close()
	data, err = io.ReadAll(resp.Body)
	if nil != err {
		return
	}

	logging.LogInfof("downloaded object [%s]", key)
	return
}

func (s3 *S3) RemoveObject(key string) (err error) {
	key = path.Join("repo", key)
	svc := s3.getService()
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Duration(s3.S3.Timeout)*time.Second)
	defer cancelFn()
	_, err = svc.DeleteObjectWithContext(ctx, &as3.DeleteObjectInput{
		Bucket: aws.String(s3.Conf.S3.Bucket),
		Key:    aws.String(key),
	})
	if nil != err {
		return
	}

	logging.LogInfof("removed object [%s]", key)
	return
}

func (s3 *S3) GetTags() (tags []*Ref, err error) {
	tags, err = s3.listRepoRefs("tags")
	if nil != err {
		logging.LogErrorf("list repo tags failed: %s", err)
		return
	}
	if 1 > len(tags) {
		tags = []*Ref{}
	}
	return
}

const pageSize = 32

func (s3 *S3) GetIndexes(page int) (ret []*entity.Index, pageCount, totalCount int, err error) {
	ret = []*entity.Index{}
	data, err := s3.DownloadObject("indexes-v2.json")
	if nil != err {
		if s3.isErrNotFound(err) {
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

	for i := start; i < end; i++ {
		index, getErr := s3.repoIndex(indexesJSON.Indexes[i].ID)
		if nil != err {
			logging.LogWarnf("get index [%s] failed: %s", indexesJSON.Indexes[i], getErr)
			continue
		}

		index.Files = nil // Optimize the performance of obtaining cloud snapshots https://github.com/siyuan-note/siyuan/issues/8387
		ret = append(ret, index)
	}
	return
}

func (s3 *S3) GetRefsFiles() (fileIDs []string, refs []*Ref, err error) {
	refs, err = s3.listRepoRefs("")
	if nil != err {
		logging.LogErrorf("list repo refs failed: %s", err)
		return
	}

	var files []string
	for _, ref := range refs {
		index, getErr := s3.repoIndex(ref.ID)
		if nil != getErr {
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

func (s3 *S3) GetChunks(checkChunkIDs []string) (chunkIDs []string, err error) {
	var keys []string
	for _, chunk := range checkChunkIDs {
		key := path.Join("repo", "objects", chunk[:2], chunk[2:])
		keys = append(keys, key)
	}

	notFound, err := s3.getNotFound(keys)
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

func (s3 *S3) repoIndex(id string) (ret *entity.Index, err error) {
	indexPath := path.Join("repo", "indexes", id)
	info, err := s3.statFile(indexPath)
	if nil != err {
		if s3.isErrNotFound(err) {
			err = nil
		}
		return
	}
	if 1 > info.Size {
		return
	}

	data, err := s3.DownloadObject(path.Join("indexes", id))
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

func (s3 *S3) listRepoRefs(refPrefix string) (ret []*Ref, err error) {
	svc := s3.getService()

	prefix := path.Join("repo", "refs", refPrefix)
	limit := int64(32)
	marker := ""
	for {
		output, listErr := svc.ListObjects(&as3.ListObjectsInput{
			Bucket:  &s3.Conf.S3.Bucket,
			Prefix:  &prefix,
			Marker:  &marker,
			MaxKeys: &limit,
		})
		if nil != listErr {
			return
		}

		marker = *output.Marker

		for _, entry := range output.Contents {
			filePath := strings.TrimPrefix(*entry.Key, "repo/")
			data, getErr := s3.DownloadObject(filePath)
			if nil != getErr {
				err = getErr
				return
			}

			id := string(data)
			info, statErr := s3.statFile(path.Join("repo", "indexes", id))
			if nil != statErr {
				err = statErr
				return
			}
			if 1 > info.Size {
				continue
			}

			ret = append(ret, &Ref{
				Name:    path.Base(*entry.Key),
				ID:      id,
				Updated: entry.LastModified.Format("2006-01-02 15:04:05"),
			})
		}

		if !(*output.IsTruncated) {
			break
		}
	}
	return
}

func (s3 *S3) listRepos() (ret []*Repo, err error) {
	svc := s3.getService()
	output, err := svc.ListBuckets(&as3.ListBucketsInput{})
	if nil != err {
		return
	}

	ret = []*Repo{}
	for _, bucket := range output.Buckets {
		ret = append(ret, &Repo{
			Name:    *bucket.Name,
			Size:    0,
			Updated: (*bucket.CreationDate).Format("2006-01-02 15:04:05"),
		})
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i].Name < ret[j].Name })
	return
}

func (s3 *S3) statFile(key string) (info *objectInfo, err error) {
	svc := s3.getService()

	header, err := svc.HeadObject(&as3.HeadObjectInput{
		Bucket: &s3.Conf.S3.Bucket,
		Key:    &key,
	})
	if nil != err {
		return
	}

	updated := header.LastModified.Format("2006-01-02 15:04:05")
	size := *header.ContentLength
	info = &objectInfo{
		Key:     key,
		Updated: updated,
		Size:    size,
	}
	return
}

func (s3 *S3) getNotFound(keys []string) (ret []string, err error) {
	if 1 > len(keys) {
		return
	}
	waitGroup := &sync.WaitGroup{}
	p, _ := ants.NewPoolWithFunc(8, func(arg interface{}) {
		defer waitGroup.Done()
		key := arg.(string)
		info, statErr := s3.statFile(key)
		if nil == info || nil != statErr {
			ret = append(ret, key)
		}
	})

	for _, key := range keys {
		waitGroup.Add(1)
		err = p.Invoke(key)
		if nil != err {
			logging.LogErrorf("invoke failed: %s", err)
			return
		}
	}
	waitGroup.Wait()
	p.Release()
	return
}

func (s3 *S3) getService() *as3.S3 {
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(s3.Conf.S3.AccessKey, s3.Conf.S3.SecretKey, ""),
		Endpoint:         aws.String(s3.Conf.S3.Endpoint),
		Region:           aws.String(s3.Conf.S3.Region),
		S3ForcePathStyle: aws.Bool(s3.Conf.S3.PathStyle),
		HTTPClient:       s3.HTTPClient,
	}))
	return as3.New(sess)
}

func (s3 *S3) isErrNotFound(err error) bool {
	switch err.(type) {
	case awserr.Error:
		code := err.(awserr.Error).Code()
		switch code {
		case as3.ErrCodeNoSuchKey:
			return true
		}
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") || strings.Contains(msg, "404") || strings.Contains(msg, "no such file or directory")
}
