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
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/88250/gulu"
	"github.com/panjf2000/ants/v2"
	"github.com/qiniu/go-sdk/v7/storage"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/eventbus"
	"github.com/siyuan-note/httpclient"
)

const (
	EvtSyncBeforeDownloadCloudIndexes = "repo.sync.beforeDownloadCloudIndexes"
	EvtSyncBeforeDownloadCloudFile    = "repo.sync.beforeDownloadCloudFile"
	EvtSyncBeforeUploadObject         = "repo.sync.beforeUploadObject"
	EvtSyncBeforeDownloadCloudChunk   = "repo.sync.beforeDownloadCloudChunk"
)

func (repo *Repo) Sync(cloudDir, userId, token, proxyURL, server string) (err error) {
	latest, err := repo.Latest()
	if nil != err {
		return
	}

	latestSync, err := repo.latestSync()
	if nil != err {
		return
	}

	if latest.ID == latestSync.ID {
		return
	}

	localIndexes, err := repo.getIndexes(latest.ID, latestSync.ID)
	if nil != err {
		return
	}

	// 从云端获取索引列表
	cloudIndexes, err := repo.downloadCloudIndexes(cloudDir, latestSync.ID, userId, token, proxyURL, server)
	if nil != err {
		return
	}

	// 从索引中得到去重后的文件列表
	cloudFileIDs := repo.getFileIDs(cloudIndexes)

	// 计算本地缺失的文件
	fetchFiles, err := repo.localNotFoundFiles(cloudFileIDs)
	if nil != err {
		return
	}

	// 从云端获取文件列表
	var files []*entity.File
	for _, fileID := range fetchFiles {
		var file *entity.File
		file, err = repo.downloadCloudFile(cloudDir, fileID, userId, token, proxyURL, server)
		if nil != err {
			return
		}
		files = append(files, file)
	}

	// 从文件列表中得到去重后的分块列表
	cloudChunkIDs := repo.getChunks(files)

	// 计算本地缺失的分块
	fetchChunks, err := repo.localNotFoundChunks(cloudChunkIDs)
	if nil != err {
		return
	}

	// 计算待上传云端的本地变更文件
	upsertFiles, err := repo.localUpsertFiles(latest, latestSync)
	if nil != err {
		return
	}

	// 计算待上传云端的分块
	upsertChunkIDs, err := repo.localUpsertChunkIDs(upsertFiles, cloudChunkIDs)
	if nil != err {
		return
	}

	// 上传分块
	waitGroup := &sync.WaitGroup{}
	var uploadErr error
	poolSize := 4
	if poolSize > len(upsertChunkIDs) {
		poolSize = len(upsertChunkIDs)
	}
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != uploadErr {
			return // 快速失败
		}

		upsertChunkID := arg.(string)
		filePath := path.Join("objects", upsertChunkID[:2], upsertChunkID[2:])
		uploadErr = repo.UploadObject(cloudDir, filePath, userId, token, proxyURL, server)
		if nil != uploadErr {
			return
		}
	})
	if nil != err {
		return
	}
	for _, upsertChunkID := range upsertChunkIDs {
		waitGroup.Add(1)
		p.Invoke(upsertChunkID)
	}
	waitGroup.Wait()
	p.Release()
	if nil != uploadErr {
		err = uploadErr
		return
	}

	// 上传文件
	poolSize = 4
	if poolSize > len(upsertFiles) {
		poolSize = len(upsertFiles)
	}
	p, err = ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != uploadErr {
			return // 快速失败
		}

		upsertFileID := arg.(string)
		filePath := path.Join("objects", upsertFileID[:2], upsertFileID[2:])
		uploadErr = repo.UploadObject(cloudDir, filePath, userId, token, proxyURL, server)
		if nil != uploadErr {
			return
		}
	})
	if nil != err {
		return
	}
	for _, upsertFile := range upsertFiles {
		waitGroup.Add(1)
		p.Invoke(upsertFile.ID)
	}
	waitGroup.Wait()
	p.Release()
	if nil != uploadErr {
		err = uploadErr
		return
	}

	// 从云端获取分块
	for _, chunkID := range fetchChunks {
		var chunk *entity.Chunk
		chunk, err = repo.downloadCloudChunk(cloudDir, chunkID, userId, token, proxyURL, server)
		if nil != err {
			return
		}

		// 分库入库
		if err = repo.store.PutChunk(chunk); nil != err {
			return
		}
	}

	// 文件入库
	for _, file := range files {
		if err = repo.store.PutFile(file); nil != err {
			return
		}
	}

	// 合并云端和本地索引
	var allIndexes []*entity.Index
	allIndexes = append(allIndexes, localIndexes...)
	allIndexes = append(allIndexes, cloudIndexes...)

	// 按索引时间排序
	sort.Slice(allIndexes, func(i, j int) bool {
		return allIndexes[i].Created >= allIndexes[j].Created
	})

	// 重新排列索引入库
	for i := 0; i < len(allIndexes); i++ {
		index := allIndexes[i]
		if i < len(allIndexes)-1 {
			index.Parent = allIndexes[i+1].ID
		} else {
			if "" != index.Parent {
				index.Parent = latest.ID
			}
		}
		err = repo.store.PutIndex(index)
		if nil != err {
			return
		}
	}

	// 上传索引
	err = repo.uploadIndexes(cloudDir, allIndexes, userId, token, proxyURL, server)
	if nil != err {
		return
	}

	latest = allIndexes[0]

	// 更新本地 latest
	err = repo.UpdateLatest(latest.ID)
	if nil != err {
		return
	}

	// 更新云端 latest
	cloudLatest := path.Join("refs", "latest")
	err = repo.UploadObject(cloudDir, cloudLatest, userId, token, proxyURL, server)
	if nil != err {
		return
	}

	// 更新本地同步点
	err = repo.UpdateLatestSync(latest.ID)
	return
}

func (repo *Repo) uploadIndexes(cloudDir string, indexes []*entity.Index, userId, token, proxyURL, server string) (err error) {
	waitGroup := &sync.WaitGroup{}
	var uploadErr error
	poolSize := 4
	if poolSize > len(indexes) {
		poolSize = len(indexes)
	}
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != uploadErr {
			return // 快速失败
		}

		indexID := arg.(string)
		uploadErr = repo.UploadObject(cloudDir, path.Join("indexes", indexID), userId, token, proxyURL, server)
		if nil != uploadErr {
			return
		}
	})
	if nil != err {
		return
	}
	for _, index := range indexes {
		waitGroup.Add(1)
		p.Invoke(index.ID)
	}
	waitGroup.Wait()
	p.Release()
	if nil != uploadErr {
		err = uploadErr
		return
	}
	return
}

func (repo *Repo) localNotFoundChunks(chunkIDs []string) (ret []string, err error) {
	for _, chunkID := range chunkIDs {
		if _, getChunkErr := repo.store.Stat(chunkID); nil != getChunkErr {
			if os.IsNotExist(getChunkErr) {
				ret = append(ret, chunkID)
				continue
			}
			err = getChunkErr
			return
		}
	}
	ret = gulu.Str.RemoveDuplicatedElem(ret)
	return
}

func (repo *Repo) localNotFoundFiles(fileIDs []string) (ret []string, err error) {
	for _, fileID := range fileIDs {
		if _, getFileErr := repo.store.Stat(fileID); nil != getFileErr {
			if os.IsNotExist(getFileErr) {
				ret = append(ret, fileID)
				continue
			}
			err = getFileErr
			return
		}
	}
	ret = gulu.Str.RemoveDuplicatedElem(ret)
	return
}

func (repo *Repo) getChunks(files []*entity.File) (chunkIDs []string) {
	for _, file := range files {
		chunkIDs = append(chunkIDs, file.Chunks...)
	}
	chunkIDs = gulu.Str.RemoveDuplicatedElem(chunkIDs)
	return
}

func (repo *Repo) getFileIDs(indexes []*entity.Index) (fileIDs []string) {
	for _, index := range indexes {
		fileIDs = append(fileIDs, index.Files...)
	}
	fileIDs = gulu.Str.RemoveDuplicatedElem(fileIDs)
	return
}

func (repo *Repo) localUpsertChunkIDs(localFiles []*entity.File, cloudChunkIDs []string) (ret []string, err error) {
	chunks := map[string]bool{}
	for _, file := range localFiles {
		for _, chunkID := range file.Chunks {
			chunks[chunkID] = true
		}
	}

	for _, cloudChunkID := range cloudChunkIDs {
		delete(chunks, cloudChunkID)
	}

	for chunkID := range chunks {
		ret = append(ret, chunkID)
	}
	return
}

func (repo *Repo) localUpsertFiles(latest, latestSync *entity.Index) (ret []*entity.File, err error) {
	files := map[string]bool{}
	for {
		for _, file := range latest.Files {
			files[file] = true
		}

		if latest.Parent == latestSync.ID || "" == latest.Parent {
			break
		}

		latest, err = repo.store.GetIndex(latest.Parent)
		if nil != err {
			return
		}
	}

	for _, file := range latestSync.Files {
		delete(files, file)
	}

	for fileID := range files {
		var file *entity.File
		file, err = repo.store.GetFile(fileID)
		if nil != err {
			return
		}

		ret = append(ret, file)
	}
	return
}

func (repo *Repo) UpdateLatestSync(id string) (err error) {
	refs := filepath.Join(repo.Path, "refs")
	err = os.MkdirAll(refs, 0755)
	if nil != err {
		return
	}
	err = gulu.File.WriteFileSafer(filepath.Join(refs, "latest-sync"), []byte(id), 0644)
	return
}

func (repo *Repo) latestSync() (ret *entity.Index, err error) {
	latestSync := filepath.Join(repo.Path, "refs", "latest-sync")
	if !gulu.File.IsExist(latestSync) {
		ret = &entity.Index{} // 构造一个空的索引表示没有同步点
		return
	}

	data, err := os.ReadFile(latestSync)
	if nil != err {
		return
	}
	hash := string(data)
	ret, err = repo.store.GetIndex(hash)
	return
}

func (repo *Repo) UploadObject(repoDir, filePath, userId, token, proxyURL, server string) (err error) {
	eventbus.Publish(EvtSyncBeforeUploadObject, filePath)

	key := path.Join("siyuan", userId, "repo", repoDir, filePath)
	uploadToken, err := repo.requestUploadToken(repoDir, key, userId, token, proxyURL, server, 40)
	if nil != err {
		return
	}

	absFilePath := filepath.Join(repo.Path, filePath)
	formUploader := storage.NewFormUploader(&storage.Config{UseHTTPS: true})
	ret := storage.PutRet{}
	err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, absFilePath, nil)
	if nil != err {
		time.Sleep(3 * time.Second)
		err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, absFilePath, nil)
		if nil != err {
			return
		}
	}
	return
}

func (repo *Repo) requestUploadToken(repoDir, key, userId, token, proxyURL, server string, length int64) (ret string, err error) {
	// 因为需要指定 key，所以每次上传文件都必须在云端生成 Token，否则有安全隐患

	var result map[string]interface{}
	req := httpclient.NewCloudRequest(proxyURL).
		SetResult(&result)
	req.SetBody(map[string]interface{}{
		"token":  token,
		"repo":   repoDir,
		"key":    key,
		"length": length})
	resp, err := req.Post(server + "/apis/siyuan/dejavu/getRepoUploadToken?uid=" + userId)
	if nil != err {
		err = errors.New("request repo upload token failed: " + err.Error())
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = errors.New("account authentication failed, please login again")
			return
		}
		err = errors.New("request repo upload token failed")
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = errors.New("request repo upload token failed: " + result["msg"].(string))
		return
	}

	resultData := result["data"].(map[string]interface{})
	ret = resultData["token"].(string)
	return
}

func (repo *Repo) downloadCloudChunk(repoDir, id, userId, token, proxyURL, server string) (ret *entity.Chunk, err error) {
	eventbus.Publish(EvtSyncBeforeDownloadCloudChunk, id)

	data, err := repo.downloadCloudObject(repoDir, id, userId, token, proxyURL, server)
	if nil != err {
		return
	}
	ret = &entity.Chunk{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	return
}

func (repo *Repo) downloadCloudFile(repoDir, id, userId, token, proxyURL, server string) (ret *entity.File, err error) {
	eventbus.Publish(EvtSyncBeforeDownloadCloudFile, id)

	data, err := repo.downloadCloudObject(repoDir, id, userId, token, proxyURL, server)
	if nil != err {
		return
	}
	ret = &entity.File{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	return
}

func (repo *Repo) downloadCloudObject(repoDir, id, userId, token, proxyURL, server string) (ret []byte, err error) {
	var result map[string]interface{}
	resp, err := httpclient.NewCloudRequest(proxyURL).
		SetResult(&result).
		SetBody(map[string]interface{}{"token": token, "repo": repoDir, "id": id}).
		Post(server + "/apis/siyuan/dejavu/getRepoObjectURL?uid=" + userId)
	if nil != err {
		err = errors.New("request object url failed: " + err.Error())
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = errors.New("account authentication failed, please login again")
			return
		}
		err = errors.New("request object url failed")
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = errors.New("request object url failed: " + result["msg"].(string))
		return
	}

	resultData := result["data"].(map[string]interface{})
	downloadURL := resultData["url"].(string)
	resp, err = httpclient.NewCloudFileRequest15s(proxyURL).Get(downloadURL)
	if nil != err {
		err = errors.New("download object failed")
		return
	}
	if 200 != resp.StatusCode {
		err = errors.New(fmt.Sprintf("download object failed [%d]", resp.StatusCode))
		if 404 == resp.StatusCode {
			err = errors.New("not found object")
		}
		return
	}

	ret, err = resp.ToBytes()
	if nil != err {
		err = errors.New("download read data failed")
		return
	}

	ret, err = repo.store.decodeData(ret)
	if nil != err {
		return
	}
	return
}

func (repo *Repo) downloadCloudIndexes(repoDir, latestSync, userId, token, proxyURL, server string) (indexes []*entity.Index, err error) {
	eventbus.Publish(EvtSyncBeforeDownloadCloudIndexes, latestSync)

	var result map[string]interface{}
	resp, err := httpclient.NewCloudRequest(proxyURL).
		SetResult(&result).
		SetBody(map[string]interface{}{"token": token, "repo": repoDir, "latestSync": latestSync}).
		Post(server + "/apis/siyuan/dejavu/getRepoIndexes?uid=" + userId)
	if nil != err {
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = errors.New("account authentication failed, please login again")
			return
		}
		err = errors.New("request repo index failed")
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = errors.New("request object url failed: " + result["msg"].(string))
		return
	}

	data := result["data"].(map[string]interface{})
	dataIndexes := data["indexes"].([]interface{})
	bytes, err := gulu.JSON.MarshalJSON(dataIndexes)
	if nil != err {
		return
	}
	err = gulu.JSON.UnmarshalJSON(bytes, &indexes)
	return
}
