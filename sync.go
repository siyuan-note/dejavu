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
	"strings"
	"sync"
	"time"

	"github.com/88250/gulu"
	"github.com/panjf2000/ants/v2"
	"github.com/qiniu/go-sdk/v7/storage"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/eventbus"
	"github.com/siyuan-note/filelock"
	"github.com/siyuan-note/httpclient"
)

const (
	EvtBeforeDownloadCloudIndex = "repo.beforeDownloadCloudIndex"
	EvtBeforeDownloadCloudFile  = "repo.beforeDownloadCloudFile"
	EvtBeforeDownloadCloudChunk = "repo.beforeDownloadCloudChunk"
	EvtBeforeUploadObject       = "repo.beforeUploadObject"
)

var (
	ErrCloudStorageSizeExceeded     = errors.New("cloud storage limit size exceeded")
	ErrCloudBackupCountExceeded     = errors.New("cloud backup count exceeded")
	ErrCloudNotFoundObject          = errors.New("not found object")
	ErrCloudGenerateConflictHistory = errors.New("generate conflict history failed")
	ErrCloudAuthFailed              = errors.New("account authentication failed, please login again")
)

type CloudInfo struct {
	Dir       string // 仓库目录名
	UserID    string // 用户 ID
	Token     string // 用户身份鉴权令牌
	LimitSize int64  // 存储空间限制
	ProxyURL  string // 代理服务器 URL
	Server    string // 云端接口端点
}

func (repo *Repo) Sync(cloudInfo *CloudInfo, context map[string]interface{}) (latest *entity.Index, mergeUpserts, mergeRemoves, mergeConflicts []*entity.File, uploadFileCount, downloadFileCount, uploadChunkCount, downloadChunkCount int, uploadBytes, downloadBytes int64, err error) {
	repo.lock.Lock()
	defer repo.lock.Unlock()

	latest, err = repo.Latest()
	if nil != err {
		return
	}

	latestSync, err := repo.latestSync()
	if nil != err {
		return
	}

	localIndexes := repo.getIndexes(latest.ID, latestSync.ID)

	// 从云端获取最新索引
	length, cloudLatest, err := repo.downloadCloudLatest(cloudInfo, context)
	if nil != err {
		if !errors.Is(err, ErrCloudNotFoundObject) {
			return
		}
	}
	downloadFileCount++
	downloadBytes += length

	if cloudLatest.ID == latest.ID {
		// 数据一致，直接返回
		return
	}

	if cloudInfo.LimitSize <= cloudLatest.Size || cloudInfo.LimitSize <= latest.Size {
		err = ErrCloudStorageSizeExceeded
		return
	}

	// 计算本地缺失的文件
	fetchFileIDs, err := repo.localNotFoundFiles(cloudLatest.Files)
	if nil != err {
		return
	}

	if 64 < len(fetchFileIDs) {
		context[CtxPushMsg] = CtxPushMsgToStatusBarAndProgress
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

	if 64 < len(fetchChunkIDs) {
		context[CtxPushMsg] = CtxPushMsgToStatusBarAndProgress
	}

	// 计算待上传云端的本地变更文件
	upsertFiles, err := repo.localUpsertFiles(localIndexes, cloudLatest.Files)
	if nil != err {
		return
	}

	// 计算待上传云端的分块
	upsertChunkIDs, err := repo.localUpsertChunkIDs(upsertFiles, cloudChunkIDs)
	if nil != err {
		return
	}

	if 64 < len(upsertChunkIDs) || 64 < len(upsertFiles) {
		context[CtxPushMsg] = CtxPushMsgToStatusBarAndProgress
	}

	// 上传分块
	length, err = repo.uploadChunks(upsertChunkIDs, cloudInfo, context)
	if nil != err {
		return
	}
	uploadChunkCount = len(upsertChunkIDs)
	uploadBytes += length

	// 上传文件
	length, err = repo.uploadFiles(upsertFiles, cloudInfo, context)
	if nil != err {
		return
	}
	uploadFileCount = len(upsertFiles)
	uploadBytes += length

	// 从云端获取分块并入库
	length, err = repo.downloadCloudChunksPut(fetchChunkIDs, cloudInfo, context)
	downloadBytes += length
	downloadChunkCount = len(fetchChunkIDs)

	// 组装还原云端最新文件列表
	cloudLatestFiles, err := repo.getFiles(cloudLatest.Files)
	if nil != err {
		return
	}

	// 计算本地相比上一个同步点的 upsert 和 remove 差异
	latestFiles, err := repo.getFiles(latest.Files)
	if nil != err {
		return
	}
	latestSyncFiles, err := repo.getFiles(latestSync.Files)
	if nil != err {
		return
	}
	localUpserts, localRemoves := repo.DiffUpsertRemove(latestFiles, latestSyncFiles)

	// 计算云端最新相比本地最新的 upsert 和 remove 差异
	var cloudUpserts, cloudRemoves []*entity.File
	if "" != cloudLatest.ID {
		cloudUpserts, cloudRemoves = repo.DiffUpsertRemove(cloudLatestFiles, latestFiles)
	}

	// 计算冲突的 upsert 和无冲突能够合并的 upsert
	// 冲突的文件以本地 upsert 和 remove 为准
	for _, cloudUpsert := range cloudUpserts {
		if repo.existDataFile(localUpserts, cloudUpsert) {
			mergeConflicts = append(mergeConflicts, cloudUpsert)
			continue
		}

		if !repo.existDataFile(localRemoves, cloudUpsert) {
			mergeUpserts = append(mergeUpserts, cloudUpsert)
		}
	}

	// 计算能够无冲突合并的 remove，冲突的文件以本地 upsert 为准
	for _, cloudRemove := range cloudRemoves {
		if !repo.existDataFile(localUpserts, cloudRemove) {
			mergeRemoves = append(mergeRemoves, cloudRemove)
		}
	}

	// 冲突文件复制到数据历史文件夹
	if 0 < len(mergeConflicts) {
		now := time.Now().Format("2006-01-02-150405")
		temp := filepath.Join(repo.TempPath, "repo", "sync", "conflicts", now)
		for _, file := range mergeConflicts {
			var checkoutTmp *entity.File
			checkoutTmp, err = repo.store.GetFile(file.ID)
			if nil != err {
				return
			}

			err = repo.checkoutFile(checkoutTmp, temp, context)
			if nil != err {
				return
			}

			absPath := filepath.Join(temp, checkoutTmp.Path)
			err = repo.genSyncHistory(now, file.Path, absPath)
			if nil != err {
				err = ErrCloudGenerateConflictHistory
				return
			}
		}
	}

	// 数据变更后需要还原工作区并创建 merge 快照
	if 0 < len(mergeUpserts) || 0 < len(mergeRemoves) {
		if 0 < len(mergeUpserts) {
			// 迁出到工作区
			err = repo.checkoutFiles(mergeUpserts, context)
			if nil != err {
				return
			}
		}

		if 0 < len(mergeRemoves) {
			// 删除工作区文件
			err = repo.removeFiles(mergeRemoves, context)
			if nil != err {
				return
			}
		}

		// 创建 merge 快照
		mergeStart := time.Now()
		latest, err = repo.index("[Sync] Cloud sync merge", context)
		if nil != err {
			return
		}
		mergeElapsed := time.Since(mergeStart)
		mergeMemo := fmt.Sprintf("[Sync] Cloud sync merge, completed in %.2fs", mergeElapsed.Seconds())
		latest.Memo = mergeMemo
		_ = repo.store.PutIndex(latest)
		localIndexes = append([]*entity.Index{latest}, localIndexes...)
	}

	// 上传索引
	length, err = repo.uploadIndexes(localIndexes, cloudInfo, context)
	if nil != err {
		return
	}
	uploadBytes += length

	// 更新本地 latest
	err = repo.UpdateLatest(latest.ID)
	if nil != err {
		return
	}

	// 更新云端 latest
	length, err = repo.uploadObject(path.Join("refs", "latest"), cloudInfo, context)
	if nil != err {
		return
	}

	// 更新本地同步点
	err = repo.UpdateLatestSync(latest.ID)
	return
}

func (repo *Repo) downloadCloudChunksPut(chunkIDs []string, cloudInfo *CloudInfo, context map[string]interface{}) (downloadBytes int64, err error) {
	waitGroup := &sync.WaitGroup{}
	var downloadErr error
	poolSize := 4
	if poolSize > len(chunkIDs) {
		poolSize = len(chunkIDs)
	}
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != downloadErr {
			return // 快速失败
		}

		chunkID := arg.(string)
		var length int64
		var chunk *entity.Chunk
		length, chunk, downloadErr = repo.downloadCloudChunk(chunkID, cloudInfo, context)
		if nil != downloadErr {
			return
		}
		if err = repo.store.PutChunk(chunk); nil != err {
			return
		}
		downloadBytes += length
	})
	if nil != err {
		return
	}
	for _, chunkID := range chunkIDs {
		waitGroup.Add(1)
		if err = p.Invoke(chunkID); nil != err {
			return
		}
	}
	waitGroup.Wait()
	p.Release()
	if nil != downloadErr {
		err = downloadErr
		return
	}
	return
}

func (repo *Repo) downloadCloudFilesPut(fileIDs []string, cloudInfo *CloudInfo, context map[string]interface{}) (downloadBytes int64, ret []*entity.File, err error) {
	lock := &sync.Mutex{}
	waitGroup := &sync.WaitGroup{}
	var downloadErr error
	poolSize := 4
	if poolSize > len(fileIDs) {
		poolSize = len(fileIDs)
	}
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != downloadErr {
			return // 快速失败
		}

		fileID := arg.(string)
		var length int64
		var file *entity.File
		length, file, downloadErr = repo.downloadCloudFile(fileID, cloudInfo, context)
		if nil != downloadErr {
			return
		}
		if err = repo.store.PutFile(file); nil != err {
			return
		}
		downloadBytes += length

		lock.Lock()
		ret = append(ret, file)
		lock.Unlock()
	})
	if nil != err {
		return
	}
	for _, fileID := range fileIDs {
		waitGroup.Add(1)
		if err = p.Invoke(fileID); nil != err {
			return
		}
	}
	waitGroup.Wait()
	p.Release()
	if nil != downloadErr {
		err = downloadErr
		return
	}
	return
}

func (repo *Repo) removeFiles(files []*entity.File, context map[string]interface{}) (err error) {
	for _, file := range files {
		absPath := repo.absPath(file.Path)
		if err = filelock.RemoveFile(absPath); nil != err {
			return
		}
		eventbus.Publish(EvtCheckoutRemoveFile, context, file.Path)
	}
	return
}

func (repo *Repo) checkoutFiles(files []*entity.File, context map[string]interface{}) (err error) {
	for _, file := range files {
		err = repo.checkoutFile(file, repo.DataPath, context)
		if nil != err {
			return
		}
	}
	return
}

func (repo *Repo) checkoutFile(file *entity.File, checkoutDir string, context map[string]interface{}) (err error) {
	var data []byte
	for _, c := range file.Chunks {
		chunk, getErr := repo.store.GetChunk(c)
		if nil != getErr {
			err = getErr
			return
		}
		data = append(data, chunk.Data...)
	}

	absPath := filepath.Join(checkoutDir, file.Path)
	dir := filepath.Dir(absPath)

	if err = os.MkdirAll(dir, 0755); nil != err {
		return
	}

	if err = filelock.NoLockFileWrite(absPath, data); nil != err {
		return
	}

	updated := time.UnixMilli(file.Updated)
	if err = os.Chtimes(absPath, updated, updated); nil != err {
		return
	}
	eventbus.Publish(EvtCheckoutUpsertFile, context, file.Path)
	return
}

func (repo *Repo) existDataFile(files []*entity.File, file *entity.File) bool {
	for _, f := range files {
		if f.ID == file.ID || f.Path == file.Path {
			return true
		}
	}
	return false
}

func (repo *Repo) getFiles(fileIDs []string) (ret []*entity.File, err error) {
	for _, fileID := range fileIDs {
		file, getErr := repo.store.GetFile(fileID)
		if nil != getErr {
			err = getErr
			return
		}
		ret = append(ret, file)
	}
	return
}

func (repo *Repo) uploadIndexes(indexes []*entity.Index, cloudInfo *CloudInfo, context map[string]interface{}) (uploadBytes int64, err error) {
	if 1 > len(indexes) {
		return
	}

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
		var length int64
		length, uploadErr = repo.uploadObject(path.Join("indexes", indexID), cloudInfo, context)
		if nil != uploadErr {
			return
		}
		uploadBytes += length
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

func (repo *Repo) uploadFiles(upsertFiles []*entity.File, cloudInfo *CloudInfo, context map[string]interface{}) (uploadBytes int64, err error) {
	if 1 > len(upsertFiles) {
		return
	}

	waitGroup := &sync.WaitGroup{}
	poolSize := 4
	if poolSize > len(upsertFiles) {
		poolSize = len(upsertFiles)
	}
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != err {
			return // 快速失败
		}

		upsertFileID := arg.(string)
		filePath := path.Join("objects", upsertFileID[:2], upsertFileID[2:])
		var length int64
		length, err = repo.uploadObject(filePath, cloudInfo, context)
		if nil != err {
			return
		}
		uploadBytes += length
	})
	if nil != err {
		return
	}
	for _, upsertFile := range upsertFiles {
		waitGroup.Add(1)
		err = p.Invoke(upsertFile.ID)
		if nil != err {
			return
		}
	}
	waitGroup.Wait()
	p.Release()
	return
}

func (repo *Repo) uploadChunks(upsertChunkIDs []string, cloudInfo *CloudInfo, context map[string]interface{}) (uploadBytes int64, err error) {
	if 1 > len(upsertChunkIDs) {
		return
	}

	waitGroup := &sync.WaitGroup{}
	poolSize := 4
	if poolSize > len(upsertChunkIDs) {
		poolSize = len(upsertChunkIDs)
	}
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != err {
			return // 快速失败
		}

		upsertChunkID := arg.(string)
		filePath := path.Join("objects", upsertChunkID[:2], upsertChunkID[2:])
		var length int64
		length, err = repo.uploadObject(filePath, cloudInfo, context)
		if nil != err {
			return
		}
		uploadBytes += length
	})
	if nil != err {
		return
	}
	for _, upsertChunkID := range upsertChunkIDs {
		waitGroup.Add(1)
		err = p.Invoke(upsertChunkID)
		if nil != err {
			return
		}
	}
	waitGroup.Wait()
	p.Release()
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

func (repo *Repo) localUpsertFiles(localIndexes []*entity.Index, cloudFileIDs []string) (ret []*entity.File, err error) {
	files := map[string]bool{}
	for _, index := range localIndexes {
		for _, file := range index.Files {
			files[file] = true
		}
	}

	for _, cloudFileID := range cloudFileIDs {
		delete(files, cloudFileID)
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
	if os.IsNotExist(err) {
		err = nil
		ret = &entity.Index{}
		return
	}
	return
}

func (repo *Repo) uploadObject(filePath string, cloudInfo *CloudInfo, ctx map[string]interface{}) (length int64, err error) {
	eventbus.Publish(EvtBeforeUploadObject, ctx, filePath)

	key := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, filePath)
	uploadToken, err := repo.requestUploadToken(key, 40, cloudInfo)
	if nil != err {
		return
	}

	absFilePath := filepath.Join(repo.Path, filePath)
	info, err := os.Stat(absFilePath)
	if nil != err {
		return
	}
	length = info.Size()

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

func (repo *Repo) requestUploadToken(key string, length int64, cloudInfo *CloudInfo) (ret string, err error) {
	// 因为需要指定 key，所以每次上传文件都必须在云端生成 Token，否则有安全隐患

	var result map[string]interface{}
	req := httpclient.NewCloudRequest(cloudInfo.ProxyURL).
		SetResult(&result)
	req.SetBody(map[string]interface{}{
		"token":  cloudInfo.Token,
		"repo":   cloudInfo.Dir,
		"key":    key,
		"length": length})
	resp, err := req.Post(cloudInfo.Server + "/apis/siyuan/dejavu/getRepoUploadToken?uid=" + cloudInfo.UserID)
	if nil != err {
		err = errors.New("request repo upload token failed: " + err.Error())
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = errors.New(fmt.Sprintf("request repo upload token failed [%d]", resp.StatusCode))
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

func (repo *Repo) downloadCloudChunk(id string, cloudInfo *CloudInfo, context map[string]interface{}) (length int64, ret *entity.Chunk, err error) {
	eventbus.Publish(EvtBeforeDownloadCloudChunk, context, id)

	key := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, "objects", id[:2], id[2:])
	data, err := repo.downloadCloudObject(key, cloudInfo)
	if nil != err {
		return
	}
	length = int64(len(data))
	ret = &entity.Chunk{ID: id, Data: data}
	return
}

func (repo *Repo) downloadCloudFile(id string, cloudInfo *CloudInfo, context map[string]interface{}) (length int64, ret *entity.File, err error) {
	eventbus.Publish(EvtBeforeDownloadCloudFile, context, id)

	key := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, "objects", id[:2], id[2:])
	data, err := repo.downloadCloudObject(key, cloudInfo)
	if nil != err {
		return
	}
	length = int64(len(data))
	ret = &entity.File{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	return
}

func (repo *Repo) downloadCloudObject(key string, cloudInfo *CloudInfo) (ret []byte, err error) {
	var result map[string]interface{}
	resp, err := httpclient.NewCloudRequest(cloudInfo.ProxyURL).
		SetResult(&result).
		SetBody(map[string]interface{}{"token": cloudInfo.Token, "repo": cloudInfo.Dir, "key": key}).
		Post(cloudInfo.Server + "/apis/siyuan/dejavu/getRepoObjectURL?uid=" + cloudInfo.UserID)
	if nil != err {
		err = errors.New("request object url failed: " + err.Error())
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = errors.New(fmt.Sprintf("request object url failed [%d]", resp.StatusCode))
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = errors.New("request object url failed: " + result["msg"].(string))
		return
	}

	resultData := result["data"].(map[string]interface{})
	downloadURL := resultData["url"].(string)
	resp, err = httpclient.NewCloudFileRequest15s(cloudInfo.ProxyURL).Get(downloadURL)
	if nil != err {
		err = errors.New("download object failed")
		return
	}
	if 200 != resp.StatusCode {
		err = errors.New(fmt.Sprintf("download object failed [%d]", resp.StatusCode))
		if 404 == resp.StatusCode {
			err = ErrCloudNotFoundObject
		}
		return
	}

	ret, err = resp.ToBytes()
	if nil != err {
		err = errors.New("download read data failed")
		return
	}

	if strings.Contains(key, "objects") {
		ret, err = repo.store.decodeData(ret)
		if nil != err {
			return
		}
	} else if strings.Contains(key, "indexes") {
		ret, err = repo.store.compressDecoder.DecodeAll(ret, nil)
	}
	return
}

func (repo *Repo) downloadCloudIndex(id string, cloudInfo *CloudInfo, context map[string]interface{}) (downloadBytes int64, index *entity.Index, err error) {
	eventbus.Publish(EvtBeforeDownloadCloudIndex, context)
	index = &entity.Index{}

	key := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, "indexes", id)
	data, err := repo.downloadCloudObject(key, cloudInfo)
	if nil != err {
		return
	}
	err = gulu.JSON.UnmarshalJSON(data, index)
	if nil != err {
		return
	}
	downloadBytes += int64(len(data))
	return
}

func (repo *Repo) downloadCloudLatest(cloudInfo *CloudInfo, context map[string]interface{}) (downloadBytes int64, index *entity.Index, err error) {
	eventbus.Publish(EvtBeforeDownloadCloudIndex, context)
	index = &entity.Index{}

	key := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, "refs", "latest")
	latestID, err := repo.downloadCloudObject(key, cloudInfo)
	if nil != err {
		return
	}
	key = path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, "indexes", string(latestID))
	data, err := repo.downloadCloudObject(key, cloudInfo)
	if nil != err {
		return
	}
	err = gulu.JSON.UnmarshalJSON(data, index)
	if nil != err {
		return
	}
	downloadBytes += int64(len(data))
	return
}

func (repo *Repo) genSyncHistory(now, relPath, absPath string) (err error) {
	historyDir, err := repo.getHistoryDirNow(now, "sync")
	if nil != err {
		return
	}

	historyPath := filepath.Join(historyDir, relPath)
	if err = gulu.File.Copy(absPath, historyPath); nil != err {
		return
	}
	return
}

func (repo *Repo) getHistoryDirNow(now, suffix string) (ret string, err error) {
	ret = filepath.Join(repo.HistoryPath, now+"-"+suffix)
	err = os.MkdirAll(ret, 0755)
	return
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 以下是仓库管理接口

func RemoveCloudRepo(name string, cloudInfo *CloudInfo) (err error) {
	request := httpclient.NewCloudRequest(cloudInfo.ProxyURL)
	resp, err := request.
		SetBody(map[string]string{"name": name, "token": cloudInfo.Token}).
		Post(cloudInfo.Server + "/apis/siyuan/dejavu/removeRepo")
	if nil != err {
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = errors.New(fmt.Sprintf("remove cloud repo failed [%d]", resp.StatusCode))
		return
	}
	return
}

func CreateCloudRepo(name string, cloudInfo *CloudInfo) (err error) {
	result := map[string]interface{}{}
	request := httpclient.NewCloudRequest(cloudInfo.ProxyURL)
	resp, err := request.
		SetResult(&result).
		SetBody(map[string]string{"name": name, "token": cloudInfo.Token}).
		Post(cloudInfo.Server + "/apis/siyuan/dejavu/createRepo")
	if nil != err {
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = errors.New(fmt.Sprintf("create cloud repo failed [%d]", resp.StatusCode))
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = errors.New(fmt.Sprintf("create cloud repo failed: %s", result["msg"]))
		return
	}
	return
}

func GetCloudRepos(cloudInfo *CloudInfo) (repos []map[string]interface{}, size int64, err error) {
	result := map[string]interface{}{}
	request := httpclient.NewCloudRequest(cloudInfo.ProxyURL)
	resp, err := request.
		SetBody(map[string]interface{}{"token": cloudInfo.Token}).
		SetResult(&result).
		Post(cloudInfo.Server + "/apis/siyuan/dejavu/getRepos?uid=" + cloudInfo.UserID)
	if nil != err {
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = errors.New(fmt.Sprintf("request cloud repo list failed [%d]", resp.StatusCode))
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = errors.New("request cloud repo list failed: " + result["msg"].(string))
		return
	}

	data := result["data"].(map[string]interface{})
	retRepos := data["repos"].([]interface{})
	for _, d := range retRepos {
		repos = append(repos, d.(map[string]interface{}))
	}
	sort.Slice(repos, func(i, j int) bool { return repos[i]["name"].(string) < repos[j]["name"].(string) })
	size = int64(data["size"].(float64))
	return
}
