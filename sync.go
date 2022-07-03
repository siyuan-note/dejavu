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
	EvtSyncBeforeDownloadCloudLatest = "repo.sync.beforeDownloadCloudLatest"
	EvtSyncAfterDownloadCloudLatest  = "repo.sync.afterDownloadCloudLatest"
	EvtSyncBeforeDownloadCloudFile   = "repo.sync.beforeDownloadCloudFile"
	EvtSyncBeforeDownloadCloudChunk  = "repo.sync.beforeDownloadCloudChunk"
	EvtSyncBeforeUploadObject        = "repo.sync.beforeUploadObject"
)

var (
	ErrSyncCloudStorageSizeExceeded = errors.New("cloud storage limit size exceeded")
	ErrSyncNotFoundObject           = errors.New("not found object")
	ErrSyncGenerateConflictHistory  = errors.New("generate conflict history failed")
)

type CloudInfo struct {
	Dir       string // 仓库目录名
	UserID    string // 用户 ID
	Token     string // 用户身份鉴权令牌
	LimitSize int64  // 存储空间限制
	ProxyURL  string // 代理服务器 URL
	Server    string // 云端接口端点
}

func (repo *Repo) Sync(cloudInfo *CloudInfo, context map[string]interface{}) (latest *entity.Index, mergeUpserts, mergeRemoves, mergeConflicts []*entity.File, err error) {
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
	cloudLatest, err := repo.downloadCloudLatest(cloudInfo, context)
	if nil != err {
		if !errors.Is(err, ErrSyncNotFoundObject) {
			return
		}
	}

	if cloudLatest.ID == latest.ID {
		// 数据一致，直接返回
		return
	}

	if cloudInfo.LimitSize <= cloudLatest.Size {
		err = ErrSyncCloudStorageSizeExceeded
		return
	}

	// 计算本地缺失的文件
	fetchFileIDs, err := repo.localNotFoundFiles(cloudLatest.Files)
	if nil != err {
		return
	}

	// 从云端下载缺失文件
	fetchedFiles, err := repo.downloadCloudFiles(fetchFileIDs, cloudInfo, context)
	if nil != err {
		return
	}

	// 从文件列表中得到去重后的分块列表
	cloudChunkIDs := repo.getChunks(fetchedFiles)

	// 计算本地缺失的分块
	fetchChunkIDs, err := repo.localNotFoundChunks(cloudChunkIDs)
	if nil != err {
		return
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

	// 上传分块
	err = repo.uploadChunks(upsertChunkIDs, cloudInfo, context)
	if nil != err {
		return
	}

	// 上传文件
	err = repo.uploadFiles(upsertFiles, cloudInfo, context)
	if nil != err {
		return
	}

	// 从云端获取分块并入库
	for _, chunkID := range fetchChunkIDs {
		var chunk *entity.Chunk
		chunk, err = repo.downloadCloudChunk(chunkID, cloudInfo, context)
		if nil != err {
			return
		}

		if err = repo.store.PutChunk(chunk); nil != err {
			return
		}
	}

	// 云端缺失文件入库
	for _, file := range fetchedFiles {
		if err = repo.store.PutFile(file); nil != err {
			return
		}
	}

	// 组装还原云端最新文件列表
	var cloudLatestFiles []*entity.File
	if nil != cloudLatest {
		cloudLatestFiles, err = repo.getFiles(cloudLatest.Files)
		if nil != err {
			return
		}
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
	cloudUpserts, cloudRemoves := repo.DiffUpsertRemove(cloudLatestFiles, latestFiles)

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
				err = ErrSyncGenerateConflictHistory
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
		latest, err = repo.index("[Auto] Cloud sync merge", context)
		if nil != err {
			return
		}
		mergeElapsed := time.Since(mergeStart)
		mergeMemo := fmt.Sprintf("[Auto] Cloud sync merge, completed in %.2fs", mergeElapsed.Seconds())
		latest.Memo = mergeMemo
		_ = repo.store.PutIndex(latest)
		localIndexes = append([]*entity.Index{latest}, localIndexes...)
	}

	// 合并云端和本地索引
	var allIndexes []*entity.Index
	allIndexes = append(allIndexes, localIndexes...)
	allIndexes = append(allIndexes, cloudLatest)
	allIndexes = removeDuplicatedIndexes(allIndexes)

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
			if index.ID != latestSync.ID {
				index.Parent = latestSync.ID
			}
		}
		err = repo.store.PutIndex(index)
		if nil != err {
			return
		}
	}

	// 上传索引
	err = repo.uploadIndexes(allIndexes, cloudInfo, context)
	if nil != err {
		return
	}

	if 1 > len(allIndexes) {
		// 意外情况下没有本地和云端索引
		return
	}

	// 以合并后的第一个索引作为 latest
	latest = allIndexes[0]

	// 更新本地 latest
	err = repo.UpdateLatest(latest.ID)
	if nil != err {
		return
	}

	// 更新云端 latest
	err = repo.uploadObject(path.Join("refs", "latest"), cloudInfo, context)
	if nil != err {
		return
	}

	// 更新本地同步点
	err = repo.UpdateLatestSync(latest.ID)
	return
}

func (repo *Repo) downloadCloudFiles(fileIDs []string, cloudInfo *CloudInfo, context map[string]interface{}) (ret []*entity.File, err error) {
	for _, fileID := range fileIDs {
		var file *entity.File
		file, err = repo.downloadCloudFile(fileID, cloudInfo, context)
		if nil != err {
			return
		}
		ret = append(ret, file)
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

func (repo *Repo) uploadIndexes(indexes []*entity.Index, cloudInfo *CloudInfo, context map[string]interface{}) (err error) {
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
		uploadErr = repo.uploadObject(path.Join("indexes", indexID), cloudInfo, context)
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

func (repo *Repo) uploadFiles(upsertFiles []*entity.File, cloudInfo *CloudInfo, context map[string]interface{}) (err error) {
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
		err = repo.uploadObject(filePath, cloudInfo, context)
		if nil != err {
			return
		}
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

func (repo *Repo) uploadChunks(upsertChunkIDs []string, cloudInfo *CloudInfo, context map[string]interface{}) (err error) {
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
		err = repo.uploadObject(filePath, cloudInfo, context)
		if nil != err {
			return
		}
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
	return
}

func (repo *Repo) uploadObject(filePath string, cloudInfo *CloudInfo, ctx map[string]interface{}) (err error) {
	eventbus.Publish(EvtSyncBeforeUploadObject, ctx, filePath)

	key := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, filePath)
	uploadToken, err := repo.requestUploadToken(key, 40, cloudInfo)
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

func (repo *Repo) downloadCloudChunk(id string, cloudInfo *CloudInfo, context map[string]interface{}) (ret *entity.Chunk, err error) {
	eventbus.Publish(EvtSyncBeforeDownloadCloudChunk, context, id)

	key := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, "objects", id[:2], id[2:])
	data, err := repo.downloadCloudObject(key, cloudInfo)
	if nil != err {
		return
	}
	ret = &entity.Chunk{ID: id, Data: data}
	return
}

func (repo *Repo) downloadCloudFile(id string, cloudInfo *CloudInfo, context map[string]interface{}) (ret *entity.File, err error) {
	eventbus.Publish(EvtSyncBeforeDownloadCloudFile, context, id)

	key := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, "objects", id[:2], id[2:])
	data, err := repo.downloadCloudObject(key, cloudInfo)
	if nil != err {
		return
	}
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
	resp, err = httpclient.NewCloudFileRequest15s(cloudInfo.ProxyURL).Get(downloadURL)
	if nil != err {
		err = errors.New("download object failed")
		return
	}
	if 200 != resp.StatusCode {
		err = errors.New(fmt.Sprintf("download object failed [%d]", resp.StatusCode))
		if 404 == resp.StatusCode {
			err = ErrSyncNotFoundObject
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

func (repo *Repo) downloadCloudLatest(cloudInfo *CloudInfo, context map[string]interface{}) (index *entity.Index, err error) {
	eventbus.Publish(EvtSyncBeforeDownloadCloudLatest, context)
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

	eventbus.Publish(EvtSyncAfterDownloadCloudLatest, context, index)
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

func removeDuplicatedIndexes(indexes []*entity.Index) (ret []*entity.Index) {
	allKeys := make(map[string]bool)
	for _, item := range indexes {
		if _, value := allKeys[item.ID]; !value {
			allKeys[item.ID] = true
			ret = append(ret, item)
		}
	}
	return
}
