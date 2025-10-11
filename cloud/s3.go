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
	"context"
	"errors"
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
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	as3 "github.com/aws/aws-sdk-go-v2/service/s3"
	as3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
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
	_, err = svc.PutObject(ctx, &as3.PutObjectInput{
		Bucket:       aws.String(s3.Conf.S3.Bucket),
		Key:          aws.String(key),
		CacheControl: aws.String("no-cache"),
		Body:         file,
	})
	if nil != err {
		return
	}

	//logging.LogInfof("uploaded object [%s]", key)
	return
}

func (s3 *S3) UploadBytes(filePath string, data []byte, overwrite bool) (length int64, err error) {
	length = int64(len(data))
	svc := s3.getService()
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Duration(s3.S3.Timeout)*time.Second)
	defer cancelFn()

	key := path.Join("repo", filePath)
	_, err = svc.PutObject(ctx, &as3.PutObjectInput{
		Bucket:       aws.String(s3.Conf.S3.Bucket),
		Key:          aws.String(key),
		CacheControl: aws.String("no-cache"),
		Body:         bytes.NewReader(data),
	})
	if nil != err {
		return
	}

	//logging.LogInfof("uploaded object [%s]", key)
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
	resp, err := svc.GetObject(ctx, input)
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

	//logging.LogInfof("downloaded object [%s]", key)
	return
}

func (s3 *S3) RemoveObject(key string) (err error) {
	key = path.Join("repo", key)
	svc := s3.getService()
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Duration(s3.S3.Timeout)*time.Second)
	defer cancelFn()
	_, err = svc.DeleteObject(ctx, &as3.DeleteObjectInput{
		Bucket: aws.String(s3.Conf.S3.Bucket),
		Key:    aws.String(key),
	})
	if nil != err {
		return
	}

	//logging.LogInfof("removed object [%s]", key)
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
		if nil != getErr {
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

func (s3 *S3) GetChunks(checkChunkIDs []string) (chunkIDs []string, err error) {
	var keys []string
	repoObjects := path.Join("repo", "objects")
	for _, chunk := range checkChunkIDs {
		key := path.Join(repoObjects, chunk[:2], chunk[2:])
		keys = append(keys, key)
	}

	notFound, err := s3.getNotFound(keys)
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

func (s3 *S3) GetIndex(id string) (index *entity.Index, err error) {
	index, err = s3.repoIndex(id)
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

func (s3 *S3) GetConcurrentReqs() (ret int) {
	ret = s3.S3.ConcurrentReqs
	if 1 > ret {
		ret = 8
	}
	if 16 < ret {
		ret = 16
	}
	return
}

func (s3 *S3) ListObjects(pathPrefix string) (ret map[string]*entity.ObjectInfo, err error) {
	ret = map[string]*entity.ObjectInfo{}
	svc := s3.getService()

	endWithSlash := strings.HasSuffix(pathPrefix, "/")
	pathPrefix = path.Join("repo", pathPrefix)
	if endWithSlash {
		pathPrefix += "/"
	}
	limit := int32(1000)
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Duration(s3.S3.Timeout)*time.Second)
	defer cancelFn()

	paginator := as3.NewListObjectsV2Paginator(svc, &as3.ListObjectsV2Input{
		Bucket:  &s3.Conf.S3.Bucket,
		Prefix:  &pathPrefix,
		MaxKeys: &limit,
	})

	for paginator.HasMorePages() {
		output, pErr := paginator.NextPage(ctx)
		if nil != pErr {
			logging.LogErrorf("list objects failed: %s", pErr)
			return nil, pErr
		}

		for _, entry := range output.Contents {
			filePath := strings.TrimPrefix(*entry.Key, pathPrefix)
			ret[filePath] = &entity.ObjectInfo{
				Path: filePath,
				Size: *entry.Size,
			}
		}
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
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Duration(s3.S3.Timeout)*time.Second)
	defer cancelFn()

	prefix := path.Join("repo", "refs", refPrefix)
	limit := int32(32)
	marker := ""
	for {
		output, listErr := svc.ListObjects(ctx, &as3.ListObjectsInput{
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
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Duration(s3.S3.Timeout)*time.Second)
	defer cancelFn()

	output, err := svc.ListBuckets(ctx, &as3.ListBucketsInput{})
	if nil != err {
		return
	}

	ret = []*Repo{}
	for _, bucket := range output.Buckets {
		if *bucket.Name != s3.S3.Bucket {
			continue
		}

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
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Duration(s3.S3.Timeout)*time.Second)
	defer cancelFn()

	header, err := svc.HeadObject(ctx, &as3.HeadObjectInput{
		Bucket: &s3.Conf.S3.Bucket,
		Key:    &key,
	})
	if nil != err {
		return
	}

	updated := time.Now().Format("2006-01-02 15:04:05")
	info = &objectInfo{Key: key, Updated: updated, Size: 0}
	if nil == header {
		logging.LogWarnf("stat file [%s] header is nil", key)
		return
	}
	info.Size = *header.ContentLength
	if nil != header.LastModified {
		updated = header.LastModified.Format("2006-01-02 15:04:05")
	}
	info.Updated = updated
	return
}

func (s3 *S3) getNotFound(keys []string) (ret []string, err error) {
	if 1 > len(keys) {
		return
	}

	poolSize := s3.GetConcurrentReqs()
	if poolSize > len(keys) {
		poolSize = len(keys)
	}

	waitGroup := &sync.WaitGroup{}
	p, _ := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
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

func (s3 *S3) getService() *as3.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		logging.LogErrorf("load default config failed: %s", err)
	}

	return as3.NewFromConfig(cfg, func(o *as3.Options) {
		o.Credentials = aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(s3.Conf.S3.AccessKey, s3.Conf.S3.SecretKey, ""))
		o.BaseEndpoint = aws.String(s3.Conf.S3.Endpoint)
		o.Region = s3.Conf.S3.Region
		o.UsePathStyle = s3.Conf.S3.PathStyle
		o.HTTPClient = s3.HTTPClient
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
}

func (s3 *S3) isErrNotFound(err error) bool {
	var nsk *as3Types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}

	var nf *as3Types.NotFound
	if errors.As(err, &nf) {
		return true
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		msg := strings.ToLower(apiErr.ErrorMessage())
		return strings.Contains(msg, "does not exist") || strings.Contains(msg, "404") || strings.Contains(msg, "no such file or directory")
	}
	return false
}
