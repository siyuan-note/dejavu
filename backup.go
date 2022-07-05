// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
//
// DejaVu is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//         http://license.coscl.org.cn/MulanPSL2
//
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
//
// See the Mulan PSL v2 for more details.

package dejavu

import (
	"errors"
	"fmt"
	"path"

	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/httpclient"
)

func (repo *Repo) DownloadTagIndex(tag, id string, cloudInfo *CloudInfo, context map[string]interface{}) (downloadFileCount, downloadChunkCount int, downloadBytes int64, err error) {
	repo.lock.Lock()
	defer repo.lock.Unlock()

	// 从云端下载标签指向的索引
	length, index, err := repo.downloadCloudIndex(id, cloudInfo, context)
	if nil != err {
		return
	}
	downloadFileCount++
	downloadBytes += length

	// 计算本地缺失的文件
	fetchFileIDs, err := repo.localNotFoundFiles(index.Files)
	if nil != err {
		return
	}

	// 从云端下载缺失文件并入库
	length, fetchedFiles, err := repo.downloadCloudFilesPut(fetchFileIDs, cloudInfo, context)
	if nil != err {
		return
	}
	downloadBytes += length
	downloadFileCount = len(fetchFileIDs)

	// 从文件列表中得到去重后的分块列表
	cloudChunkIDs := repo.getChunks(fetchedFiles)

	// 计算本地缺失的分块
	fetchChunkIDs, err := repo.localNotFoundChunks(cloudChunkIDs)
	if nil != err {
		return
	}

	// 从云端获取分块并入库
	length, err = repo.downloadCloudChunksPut(fetchChunkIDs, cloudInfo, context)
	downloadBytes += length
	downloadChunkCount = len(fetchChunkIDs)

	// 更新本地标签
	err = repo.AddTag(id, tag)
	if nil != err {
		return
	}
	return
}

func (repo *Repo) UploadTagIndex(tag, id string, cloudInfo *CloudInfo, context map[string]interface{}) (uploadFileCount, uploadChunkCount int, uploadBytes int64, err error) {
	repo.lock.Lock()
	defer repo.lock.Unlock()

	index, err := repo.store.GetIndex(id)
	if nil != err {
		return
	}

	// 从云端获取文件列表
	cloudFileIDs, err := repo.getCloudRepoRefsFiles(cloudInfo)
	if nil != err {
		return
	}

	// 计算云端缺失的文件
	var uploadFiles []*entity.File
	for _, localFileID := range index.Files {
		if !gulu.Str.Contains(localFileID, cloudFileIDs) {
			var uploadFile *entity.File
			uploadFile, err = repo.store.GetFile(localFileID)
			if nil != err {
				return
			}
			uploadFiles = append(uploadFiles, uploadFile)
		}
	}

	// 从文件列表中得到去重后的分块列表
	uploadChunkIDs := repo.getChunks(uploadFiles)

	// 计算云端缺失的分块
	uploadChunkIDs, err = repo.getCloudRepoUploadChunks(uploadChunkIDs, cloudInfo)
	if nil != err {
		return
	}

	// 上传分块
	length, err := repo.uploadChunks(uploadChunkIDs, cloudInfo, context)
	if nil != err {
		return
	}
	uploadChunkCount = len(uploadChunkIDs)
	uploadBytes += length

	// 上传文件
	length, err = repo.uploadFiles(uploadFiles, cloudInfo, context)
	if nil != err {
		return
	}
	uploadFileCount = len(uploadFiles)
	uploadBytes += length

	// 上传标签
	length, err = repo.uploadObject(path.Join("refs", "tags", tag), cloudInfo, context)
	uploadFileCount++
	uploadBytes += length
	return
}

func (repo *Repo) getCloudRepoUploadChunks(uploadChunkIDs []string, cloudInfo *CloudInfo) (chunks []string, err error) {
	result := gulu.Ret.NewResult()
	request := httpclient.NewCloudRequest(cloudInfo.ProxyURL)
	resp, err := request.
		SetResult(&result).
		SetBody(map[string]interface{}{"repo": cloudInfo.Dir, "token": cloudInfo.Token, "chunks": uploadChunkIDs}).
		Post(cloudInfo.Server + "/apis/siyuan/dejavu/getRepoUploadChunks?uid=" + cloudInfo.UserID)
	if nil != err {
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrAuthFailed
			return
		}
		err = errors.New(fmt.Sprintf("get cloud repo refs chunks failed [%d]", resp.StatusCode))
		return
	}

	if 0 != result.Code {
		err = errors.New(fmt.Sprintf("get cloud repo refs chunks failed: %s", result.Msg))
		return
	}

	retData := result.Data.(map[string]interface{})
	retChunks := retData["chunks"].([]interface{})
	for _, retChunk := range retChunks {
		chunks = append(chunks, retChunk.(string))
	}
	return
}

func (repo *Repo) getCloudRepoRefsFiles(cloudInfo *CloudInfo) (files []string, err error) {
	result := gulu.Ret.NewResult()
	request := httpclient.NewCloudRequest(cloudInfo.ProxyURL)
	resp, err := request.
		SetResult(&result).
		SetBody(map[string]string{"repo": cloudInfo.Dir, "token": cloudInfo.Token}).
		Post(cloudInfo.Server + "/apis/siyuan/dejavu/getRepoRefsFiles?uid=" + cloudInfo.UserID)
	if nil != err {
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrAuthFailed
			return
		}
		err = errors.New(fmt.Sprintf("get cloud repo refs files failed [%d]", resp.StatusCode))
		return
	}

	if 0 != result.Code {
		err = errors.New(fmt.Sprintf("get cloud repo refs files failed: %s", result.Msg))
		return
	}

	retData := result.Data.(map[string]interface{})
	retFiles := retData["files"].([]interface{})
	for _, retFile := range retFiles {
		files = append(files, retFile.(string))
	}
	return
}

func (repo *Repo) GetCloudRepoTags(cloudInfo *CloudInfo) (tags []map[string]interface{}, err error) {
	result := gulu.Ret.NewResult()
	request := httpclient.NewCloudRequest(cloudInfo.ProxyURL)
	resp, err := request.
		SetResult(&result).
		SetBody(map[string]string{"repo": cloudInfo.Dir, "token": cloudInfo.Token}).
		Post(cloudInfo.Server + "/apis/siyuan/dejavu/getRepoTags?uid=" + cloudInfo.UserID)
	if nil != err {
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrAuthFailed
			return
		}
		err = errors.New(fmt.Sprintf("get cloud repo tags failed [%d]", resp.StatusCode))
		return
	}

	if 0 != result.Code {
		err = errors.New(fmt.Sprintf("get cloud repo tags failed: %s", result.Msg))
		return
	}

	retData := result.Data.(map[string]interface{})
	retTags := retData["tags"].([]interface{})
	for _, retTag := range retTags {
		tags = append(tags, retTag.(map[string]interface{}))
	}
	return
}
