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
	"github.com/qiniu/go-sdk/v7/client"
	"github.com/qiniu/go-sdk/v7/storage"
	ignore "github.com/sabhiram/go-gitignore"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/eventbus"
	"github.com/siyuan-note/filelock"
	"github.com/siyuan-note/httpclient"
	"github.com/siyuan-note/logging"
)

const (
	EvtCloudBeforeUploadIndex    = "repo.cloudBeforeUploadIndex"
	EvtCloudBeforeUploadFiles    = "repo.cloudBeforeUploadFiles"
	EvtCloudBeforeUploadFile     = "repo.cloudBeforeUploadFile"
	EvtCloudBeforeUploadChunks   = "repo.cloudBeforeUploadChunks"
	EvtCloudBeforeUploadChunk    = "repo.cloudBeforeUploadChunk"
	EvtCloudBeforeDownloadIndex  = "repo.cloudBeforeDownloadIndex"
	EvtCloudBeforeDownloadFiles  = "repo.cloudBeforeDownloadFiles"
	EvtCloudBeforeDownloadFile   = "repo.cloudBeforeDownloadFile"
	EvtCloudBeforeDownloadChunks = "repo.cloudBeforeDownloadChunks"
	EvtCloudBeforeDownloadChunk  = "repo.cloudBeforeDownloadChunk"
	EvtCloudBeforeDownloadRef    = "repo.cloudBeforeDownloadRef"
	EvtCloudBeforeUploadRef      = "repo.cloudBeforeUploadRef"
)

var (
	ErrCloudStorageSizeExceeded     = errors.New("cloud storage limit size exceeded")
	ErrCloudBackupCountExceeded     = errors.New("cloud backup count exceeded")
	ErrCloudObjectNotFound          = errors.New("cloud object not found")
	ErrCloudGenerateConflictHistory = errors.New("generate conflict history failed")
	ErrCloudAuthFailed              = errors.New("cloud account auth failed")
)

type CloudInfo struct {
	Dir       string // ???????????????
	UserID    string // ?????? ID
	Token     string // ????????????????????????
	LimitSize int64  // ??????????????????
	Server    string // ??????????????????
}

type MergeResult struct {
	Upserts, Removes, Conflicts []*entity.File
}

type DownloadTrafficStat struct {
	DownloadFileCount  int
	DownloadChunkCount int
	DownloadBytes      int64
}

type UploadTrafficStat struct {
	UploadFileCount  int
	UploadChunkCount int
	UploadBytes      int64
}

type TrafficStat struct {
	DownloadTrafficStat
	UploadTrafficStat
}

func (repo *Repo) Sync(cloudInfo *CloudInfo, context map[string]interface{}) (latest *entity.Index, mergeResult *MergeResult, trafficStat *TrafficStat, err error) {
	lock.Lock()
	defer lock.Unlock()

	latest, mergeResult, trafficStat, err = repo.sync(cloudInfo, context)
	if e, ok := err.(*os.PathError); ok && os.IsNotExist(err) {
		p := e.Path
		if !strings.Contains(p, "objects") {
			return
		}

		// ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
		err = fmt.Errorf("repo fatal error: %s", err.Error())
	}
	return
}

func (repo *Repo) sync(cloudInfo *CloudInfo, context map[string]interface{}) (latest *entity.Index, mergeResult *MergeResult, trafficStat *TrafficStat, err error) {
	mergeResult = &MergeResult{}
	trafficStat = &TrafficStat{}

	latest, err = repo.Latest()
	if nil != err {
		logging.LogErrorf("get latest failed: %s", err)
		return
	}

	latestSync, err := repo.latestSync()
	if nil != err {
		logging.LogErrorf("get latest sync failed: %s", err)
		return
	}

	localIndexes := repo.getIndexes(latest.ID, latestSync.ID)

	// ???????????????????????????
	length, cloudLatest, err := repo.downloadCloudLatest(cloudInfo, context)
	if nil != err {
		if !errors.Is(err, ErrCloudObjectNotFound) {
			logging.LogErrorf("download cloud latest failed: %s", err)
			return
		}
	}
	trafficStat.DownloadFileCount++
	trafficStat.DownloadBytes += length

	if cloudLatest.ID == latest.ID {
		// ???????????????????????????
		return
	}

	if cloudInfo.LimitSize <= cloudLatest.Size || cloudInfo.LimitSize <= latest.Size {
		err = ErrCloudStorageSizeExceeded
		return
	}

	// ???????????????????????????
	fetchFileIDs, err := repo.localNotFoundFiles(cloudLatest.Files)
	if nil != err {
		logging.LogErrorf("get local not found files failed: %s", err)
		return
	}

	// ????????????????????????????????????
	length, fetchedFiles, err := repo.downloadCloudFilesPut(fetchFileIDs, cloudInfo, context)
	if nil != err {
		logging.LogErrorf("download cloud files put failed: %s", err)
		return
	}
	trafficStat.DownloadBytes += length
	trafficStat.DownloadFileCount = len(fetchFileIDs)

	// ????????????????????????????????????????????????
	cloudChunkIDs := repo.getChunks(fetchedFiles)

	// ???????????????????????????
	fetchChunkIDs, err := repo.localNotFoundChunks(cloudChunkIDs)
	if nil != err {
		logging.LogErrorf("get local not found chunks failed: %s", err)
		return
	}

	// ??????????????????????????????????????????
	upsertFiles, err := repo.localUpsertFiles(localIndexes, cloudLatest.Files)
	if nil != err {
		logging.LogErrorf("get local upsert files failed: %s", err)
		return
	}

	// ??????????????????????????????
	upsertChunkIDs, err := repo.localUpsertChunkIDs(upsertFiles, cloudChunkIDs)
	if nil != err {
		logging.LogErrorf("get local upsert chunk ids failed: %s", err)
		return
	}

	// ??????????????????
	latestKey := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, "refs/latest")
	keyUploadToken, scopeUploadToken, err := repo.requestScopeKeyUploadToken(latestKey, cloudInfo)
	if nil != err {
		logging.LogErrorf("request upload token failed: %s", err)
		return
	}

	// ????????????
	length, err = repo.uploadChunks(upsertChunkIDs, scopeUploadToken, cloudInfo, context)
	if nil != err {
		logging.LogErrorf("upload chunks failed: %s", err)
		return
	}
	trafficStat.UploadChunkCount = len(upsertChunkIDs)
	trafficStat.UploadBytes += length

	// ????????????
	length, err = repo.uploadFiles(upsertFiles, scopeUploadToken, cloudInfo, context)
	if nil != err {
		logging.LogErrorf("upload files failed: %s", err)
		return
	}
	trafficStat.UploadFileCount = len(upsertFiles)
	trafficStat.UploadBytes += length

	// ????????????????????????????????????
	length, err = repo.downloadCloudChunksPut(fetchChunkIDs, cloudInfo, context)
	if nil != err {
		logging.LogErrorf("download cloud chunks put failed: %s", err)
		return
	}
	trafficStat.DownloadBytes += length
	trafficStat.DownloadChunkCount = len(fetchChunkIDs)

	// ????????????????????????????????????
	cloudLatestFiles, err := repo.getFiles(cloudLatest.Files)
	if nil != err {
		logging.LogErrorf("get cloud latest files failed: %s", err)
		return
	}

	// ?????????????????????????????????????????????????????????
	//
	// ?????????????????? fetchedFiles ?????????????????????????????????????????????????????????????????????????????????????????????????????? fetchedFiles ??????????????????????????????
	// ?????????????????????????????????????????????????????????????????????????????????????????????????????????
	cloudChunkIDs = repo.getChunks(cloudLatestFiles)
	fetchChunkIDs, err = repo.localNotFoundChunks(cloudChunkIDs)
	if nil != err {
		logging.LogErrorf("get local not found chunks failed: %s", err)
		return
	}
	length, err = repo.downloadCloudChunksPut(fetchChunkIDs, cloudInfo, context)
	if nil != err {
		logging.LogErrorf("download cloud chunks put failed: %s", err)
		return
	}
	trafficStat.DownloadBytes += length
	trafficStat.DownloadChunkCount = len(fetchChunkIDs)

	// ??????????????????????????????????????? upsert ??? remove ??????
	latestFiles, err := repo.getFiles(latest.Files)
	if nil != err {
		logging.LogErrorf("get latest files failed: %s", err)
		return
	}
	latestSyncFiles, err := repo.getFiles(latestSync.Files)
	if nil != err {
		logging.LogErrorf("get latest sync files failed: %s", err)
		return
	}
	localUpserts, localRemoves := repo.DiffUpsertRemove(latestFiles, latestSyncFiles)

	// ??????????????????????????????????????? upsert ??? remove ??????
	var cloudUpserts, cloudRemoves []*entity.File
	if "" != cloudLatest.ID {
		cloudUpserts, cloudRemoves = repo.DiffUpsertRemove(cloudLatestFiles, latestFiles)
	}

	var cloudUpsertIgnore, localUpsertIgnore *entity.File
	for _, upsert := range localUpserts {
		if "/.siyuan/syncignore" == upsert.Path {
			localUpsertIgnore = upsert
			break
		}
	}

	// ??????????????? upsert ??????????????????????????? upsert
	// ???????????????????????? upsert ??? remove ??????
	for _, cloudUpsert := range cloudUpserts {
		if "/.siyuan/syncignore" == cloudUpsert.Path {
			cloudUpsertIgnore = cloudUpsert
		}

		if repo.existDataFile(localUpserts, cloudUpsert) {
			mergeResult.Conflicts = append(mergeResult.Conflicts, cloudUpsert)
			continue
		}

		if !repo.existDataFile(localRemoves, cloudUpsert) {
			mergeResult.Upserts = append(mergeResult.Upserts, cloudUpsert)
		}
	}

	// ?????????????????????????????? remove??????????????????????????? upsert ??????
	for _, cloudRemove := range cloudRemoves {
		if !repo.existDataFile(localUpserts, cloudRemove) {
			mergeResult.Removes = append(mergeResult.Removes, cloudRemove)
		}
	}

	// ????????????????????????????????????????????????????????? remove????????????????????????????????? https://github.com/siyuan-note/siyuan/issues/5497
	var ignoreLines []string
	if nil != cloudUpsertIgnore {
		coDir := filepath.Join(repo.DataPath)
		if nil != localUpsertIgnore {
			// ?????? syncignore ??????????????????????????????
			coDir = filepath.Join(repo.TempPath, "repo", "sync", "ignore")
		}
		if err = repo.checkoutFile(cloudUpsertIgnore, coDir, context); nil != err {
			logging.LogErrorf("checkout ignore file failed: %s", err)
			return
		}
		data, readErr := os.ReadFile(filepath.Join(coDir, cloudUpsertIgnore.Path))
		if nil != readErr {
			logging.LogErrorf("read ignore file failed: %s", readErr)
			err = readErr
			return
		}
		dataStr := string(data)
		dataStr = strings.ReplaceAll(dataStr, "\r\n", "\n")
		ignoreLines = strings.Split(dataStr, "\n")
		//logging.LogInfof("sync merge ignore rules: \n  %s", strings.Join(ignoreLines, "\n  "))
	}

	ignoreMatcher := ignore.CompileIgnoreLines(ignoreLines...)
	var tmp []*entity.File
	for _, remove := range mergeResult.Removes {
		if !ignoreMatcher.MatchesPath(remove.Path) {
			tmp = append(tmp, remove)
			continue
		}
		// logging.LogInfof("sync merge ignore remove [%s]", remove.Path)
	}
	mergeResult.Removes = tmp

	// ??????????????????????????????????????????
	if 0 < len(mergeResult.Conflicts) {
		now := time.Now().Format("2006-01-02-150405")
		temp := filepath.Join(repo.TempPath, "repo", "sync", "conflicts", now)
		for _, file := range mergeResult.Conflicts {
			var checkoutTmp *entity.File
			checkoutTmp, err = repo.store.GetFile(file.ID)
			if nil != err {
				logging.LogErrorf("get file failed: %s", err)
				return
			}

			err = repo.checkoutFile(checkoutTmp, temp, context)
			if nil != err {
				logging.LogErrorf("checkout file failed: %s", err)
				return
			}

			absPath := filepath.Join(temp, checkoutTmp.Path)
			err = repo.genSyncHistory(now, file.Path, absPath)
			if nil != err {
				logging.LogErrorf("generate sync history failed: %s", err)
				err = ErrCloudGenerateConflictHistory
				return
			}
		}
	}

	// ????????????????????????????????????????????? merge ??????
	if 0 < len(mergeResult.Upserts) || 0 < len(mergeResult.Removes) {
		if 0 < len(mergeResult.Upserts) {
			// ??????????????????
			err = repo.checkoutFiles(mergeResult.Upserts, context)
			if nil != err {
				logging.LogErrorf("checkout files failed: %s", err)
				return
			}
		}

		if 0 < len(mergeResult.Removes) {
			// ?????????????????????
			err = repo.removeFiles(mergeResult.Removes, context)
			if nil != err {
				logging.LogErrorf("remove files failed: %s", err)
				return
			}
		}

		// ?????? merge ??????
		mergeStart := time.Now()
		latest, err = repo.index("[Sync] Cloud sync merge", context)
		if nil != err {
			logging.LogErrorf("merge index failed: %s", err)
			return
		}
		mergeElapsed := time.Since(mergeStart)
		mergeMemo := fmt.Sprintf("[Sync] Cloud sync merge, completed in %.2fs", mergeElapsed.Seconds())
		latest.Memo = mergeMemo
		_ = repo.store.PutIndex(latest)
		localIndexes = append([]*entity.Index{latest}, localIndexes...)

		// ???????????? upserts ?????????????????????
		upsertFiles, err = repo.localUpsertFiles(localIndexes, cloudLatest.Files)
		if nil != err {
			logging.LogErrorf("get local upsert files failed: %s", err)
			return
		}

		upsertChunkIDs, err = repo.localUpsertChunkIDs(upsertFiles, cloudChunkIDs)
		if nil != err {
			logging.LogErrorf("get local upsert chunk ids failed: %s", err)
			return
		}

		length, err = repo.uploadChunks(upsertChunkIDs, scopeUploadToken, cloudInfo, context)
		if nil != err {
			logging.LogErrorf("upload chunks failed: %s", err)
			return
		}
		trafficStat.UploadChunkCount = len(upsertChunkIDs)
		trafficStat.UploadBytes += length

		length, err = repo.uploadFiles(upsertFiles, scopeUploadToken, cloudInfo, context)
		if nil != err {
			logging.LogErrorf("upload files failed: %s", err)
			return
		}
		trafficStat.UploadFileCount = len(upsertFiles)
		trafficStat.UploadBytes += length
	}

	// ????????????
	length, err = repo.uploadIndexes(localIndexes, scopeUploadToken, cloudInfo, context)
	if nil != err {
		logging.LogErrorf("upload indexes failed: %s", err)
		return
	}
	trafficStat.UploadBytes += length

	// ???????????? latest
	err = repo.UpdateLatest(latest.ID)
	if nil != err {
		logging.LogErrorf("update latest failed: %s", err)
		return
	}

	// ???????????? latest
	length, err = repo.updateCloudRef("refs/latest", keyUploadToken, cloudInfo, context)
	if nil != err {
		logging.LogErrorf("update cloud [refs/latest] failed: %s", err)
		return
	}
	trafficStat.UploadBytes += length

	// ?????????????????????
	err = repo.UpdateLatestSync(latest.ID)
	if nil != err {
		logging.LogErrorf("update latest sync failed: %s", err)
		return
	}

	// ????????????
	go repo.addTraffic(trafficStat.UploadBytes, trafficStat.DownloadBytes, cloudInfo)
	return
}

func (repo *Repo) downloadCloudChunksPut(chunkIDs []string, cloudInfo *CloudInfo, context map[string]interface{}) (downloadBytes int64, err error) {
	if 1 > len(chunkIDs) {
		return
	}

	waitGroup := &sync.WaitGroup{}
	var downloadErr error
	poolSize := 8
	if poolSize > len(chunkIDs) {
		poolSize = len(chunkIDs)
	}
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != downloadErr {
			return // ????????????
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

	eventbus.Publish(EvtCloudBeforeDownloadChunks, context, chunkIDs)
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
	if 1 > len(fileIDs) {
		return
	}

	lock := &sync.Mutex{}
	waitGroup := &sync.WaitGroup{}
	var downloadErr error
	poolSize := 8
	if poolSize > len(fileIDs) {
		poolSize = len(fileIDs)
	}
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != downloadErr {
			return // ????????????
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

	eventbus.Publish(EvtCloudBeforeDownloadFiles, context, fileIDs)
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
	eventbus.Publish(EvtCheckoutRemoveFiles, context, files)
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
	eventbus.Publish(EvtCheckoutUpsertFiles, context, files)
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
		logging.LogErrorf("change [%s] time failed: %s", absPath, err)
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

func (repo *Repo) updateCloudRef(ref, keyUploadToken string, cloudInfo *CloudInfo, context map[string]interface{}) (uploadBytes int64, err error) {
	eventbus.Publish(EvtCloudBeforeUploadRef, context, ref)

	absFilePath := filepath.Join(repo.Path, ref)
	info, err := os.Stat(absFilePath)
	if nil != err {
		return
	}
	uploadBytes = info.Size()
	err = repo.uploadObject(ref, cloudInfo, keyUploadToken)
	return
}

func (repo *Repo) uploadIndexes(indexes []*entity.Index, scopeUploadToken string, cloudInfo *CloudInfo, context map[string]interface{}) (uploadBytes int64, err error) {
	if 1 > len(indexes) {
		return
	}

	for _, index := range indexes {
		absFilePath := filepath.Join(repo.Path, "indexes", index.ID)
		info, statErr := os.Stat(absFilePath)
		if nil != statErr {
			return
		}
		length := info.Size()
		uploadBytes += length
	}

	waitGroup := &sync.WaitGroup{}
	poolSize := 4
	if poolSize > len(indexes) {
		poolSize = len(indexes)
	}
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != err {
			return // ????????????
		}

		indexID := arg.(string)
		eventbus.Publish(EvtCloudBeforeUploadIndex, context, indexID)
		if err = repo.uploadObject(path.Join("indexes", indexID), cloudInfo, scopeUploadToken); nil != err {
			return
		}
	})
	if nil != err {
		return
	}

	for _, index := range indexes {
		waitGroup.Add(1)
		if err = p.Invoke(index.ID); nil != err {
			return
		}
	}
	waitGroup.Wait()
	p.Release()
	return
}

func (repo *Repo) uploadFiles(upsertFiles []*entity.File, scopeUploadToken string, cloudInfo *CloudInfo, context map[string]interface{}) (uploadBytes int64, err error) {
	if 1 > len(upsertFiles) {
		return
	}

	for _, upsertFile := range upsertFiles {
		absFilePath := filepath.Join(repo.Path, "objects", upsertFile.ID[:2], upsertFile.ID[2:])
		info, statErr := os.Stat(absFilePath)
		if nil != statErr {
			return
		}
		length := info.Size()
		uploadBytes += length
	}

	waitGroup := &sync.WaitGroup{}
	poolSize := 8
	if poolSize > len(upsertFiles) {
		poolSize = len(upsertFiles)
	}
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != err {
			return // ????????????
		}

		upsertFileID := arg.(string)
		filePath := path.Join("objects", upsertFileID[:2], upsertFileID[2:])
		eventbus.Publish(EvtCloudBeforeUploadFile, context, upsertFileID)
		if err = repo.uploadObject(filePath, cloudInfo, scopeUploadToken); nil != err {
			return
		}
	})
	if nil != err {
		return
	}

	eventbus.Publish(EvtCloudBeforeUploadFiles, context, upsertFiles)
	for _, upsertFile := range upsertFiles {
		waitGroup.Add(1)
		if err = p.Invoke(upsertFile.ID); nil != err {
			return
		}
	}
	waitGroup.Wait()
	p.Release()
	return
}

func (repo *Repo) uploadChunks(upsertChunkIDs []string, scopeUploadToken string, cloudInfo *CloudInfo, context map[string]interface{}) (uploadBytes int64, err error) {
	if 1 > len(upsertChunkIDs) {
		return
	}

	for _, upsertChunkID := range upsertChunkIDs {
		absFilePath := filepath.Join(repo.Path, "objects", upsertChunkID[:2], upsertChunkID[2:])
		info, statErr := os.Stat(absFilePath)
		if nil != statErr {
			return
		}
		length := info.Size()
		uploadBytes += length
	}

	waitGroup := &sync.WaitGroup{}
	poolSize := 8
	if poolSize > len(upsertChunkIDs) {
		poolSize = len(upsertChunkIDs)
	}
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != err {
			return // ????????????
		}

		upsertChunkID := arg.(string)
		filePath := path.Join("objects", upsertChunkID[:2], upsertChunkID[2:])
		eventbus.Publish(EvtCloudBeforeUploadChunk, context, upsertChunkID)
		if err = repo.uploadObject(filePath, cloudInfo, scopeUploadToken); nil != err {
			return
		}
	})
	if nil != err {
		return
	}

	eventbus.Publish(EvtCloudBeforeUploadChunks, context, upsertChunkIDs)
	for _, upsertChunkID := range upsertChunkIDs {
		waitGroup.Add(1)
		if err = p.Invoke(upsertChunkID); nil != err {
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
		ret = &entity.Index{} // ?????????????????????????????????????????????
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

func (repo *Repo) uploadObject(filePath string, cloudInfo *CloudInfo, uploadToken string) (err error) {
	absFilePath := filepath.Join(repo.Path, filePath)
	key := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, filePath)
	formUploader := storage.NewFormUploader(&storage.Config{UseHTTPS: true})
	ret := storage.PutRet{}
	err = formUploader.PutFile(context.Background(), &ret, uploadToken, key, absFilePath, nil)
	if nil != err {
		if e, ok := err.(*client.ErrorInfo); ok && 614 == e.Code {
			// file exists
			logging.LogWarnf("upload object [%s] exists: %s", absFilePath, err)
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

type UploadToken struct {
	key, token string
	expired    int64
}

var (
	keyUploadTokenMap   = map[string]*UploadToken{}
	scopeUploadTokenMap = map[string]*UploadToken{}
	uploadTokenMapLock  = &sync.Mutex{}
)

func (repo *Repo) requestScopeKeyUploadToken(key string, cloudInfo *CloudInfo) (keyToken, scopeToken string, err error) {
	now := time.Now().UnixMilli()
	keyPrefix := path.Join("siyuan", cloudInfo.UserID)

	uploadTokenMapLock.Lock()
	cachedKeyToken := keyUploadTokenMap[key]
	cachedScopeToken := scopeUploadTokenMap[keyPrefix]
	if nil != cachedScopeToken && nil != cachedKeyToken {
		if now < cachedKeyToken.expired && now < cachedScopeToken.expired {
			keyToken = cachedKeyToken.token
			scopeToken = cachedScopeToken.token
			uploadTokenMapLock.Unlock()
			return
		}
		delete(keyUploadTokenMap, key)
		delete(scopeUploadTokenMap, keyPrefix)
	}
	uploadTokenMapLock.Unlock()

	var result map[string]interface{}
	req := httpclient.NewCloudRequest().SetResult(&result)
	req.SetBody(map[string]interface{}{
		"token":     cloudInfo.Token,
		"key":       key,
		"keyPrefix": keyPrefix,
	})
	resp, err := req.Post(cloudInfo.Server + "/apis/siyuan/dejavu/getRepoScopeKeyUploadToken?uid=" + cloudInfo.UserID)
	if nil != err {
		err = fmt.Errorf("request repo upload token failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("request repo upload token failed [%d]", resp.StatusCode)
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = fmt.Errorf("request repo upload token failed: %s", result["msg"].(string))
		return
	}

	resultData := result["data"].(map[string]interface{})
	keyToken = resultData["keyToken"].(string)
	scopeToken = resultData["scopeToken"].(string)
	expired := now + 1000*60*60*24 - 60*1000
	uploadTokenMapLock.Lock()
	keyUploadTokenMap[key] = &UploadToken{
		key:     key,
		token:   keyToken,
		expired: expired,
	}
	scopeUploadTokenMap[keyPrefix] = &UploadToken{
		key:     keyPrefix,
		token:   scopeToken,
		expired: expired,
	}
	uploadTokenMapLock.Unlock()
	return
}

func (repo *Repo) addTraffic(uploadBytes, downloadBytes int64, cloudInfo *CloudInfo) {
	request := httpclient.NewCloudRequest()
	resp, err := request.
		SetBody(map[string]interface{}{"token": cloudInfo.Token, "uploadBytes": uploadBytes, "downloadBytes": downloadBytes}).
		Post(cloudInfo.Server + "/apis/siyuan/dejavu/addTraffic")
	if nil != err {
		logging.LogErrorf("add traffic failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		logging.LogErrorf("add traffic failed: %d", resp.StatusCode)
		return
	}
	return
}

func (repo *Repo) downloadCloudChunk(id string, cloudInfo *CloudInfo, context map[string]interface{}) (length int64, ret *entity.Chunk, err error) {
	eventbus.Publish(EvtCloudBeforeDownloadChunk, context, id)

	key := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, "objects", id[:2], id[2:])
	data, err := repo.downloadCloudObject(key)
	if nil != err {
		logging.LogErrorf("download cloud chunk [%s] failed: %s", id, err)
		return
	}
	length = int64(len(data))
	ret = &entity.Chunk{ID: id, Data: data}
	return
}

func (repo *Repo) downloadCloudFile(id string, cloudInfo *CloudInfo, context map[string]interface{}) (length int64, ret *entity.File, err error) {
	eventbus.Publish(EvtCloudBeforeDownloadFile, context, id)

	key := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, "objects", id[:2], id[2:])
	data, err := repo.downloadCloudObject(key)
	if nil != err {
		logging.LogErrorf("download cloud file [%s] failed: %s", id, err)
		return
	}
	length = int64(len(data))
	ret = &entity.File{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	return
}

func (repo *Repo) downloadCloudObject(key string) (ret []byte, err error) {
	resp, err := httpclient.NewCloudFileRequest15s().Get("https://siyuan-data.b3logfile.com/" + key)
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

	ret, err = resp.ToBytes()
	if nil != err {
		err = fmt.Errorf("download read data failed: %s", err)
		return
	}

	if strings.Contains(key, "objects") {
		ret, err = repo.store.decodeData(ret)
		if nil != err {
			logging.LogErrorf("decode downloaded data [%s] failed: %s", key, err)
			return
		}
	} else if strings.Contains(key, "indexes") {
		ret, err = repo.store.compressDecoder.DecodeAll(ret, nil)
	}
	if nil != err {
		logging.LogErrorf("decode downloaded data [%s] failed: %s", key, err)
		return
	}
	//logging.LogInfof("downloaded object [%s]", key)
	return
}

func (repo *Repo) downloadCloudIndex(id string, cloudInfo *CloudInfo, context map[string]interface{}) (downloadBytes int64, index *entity.Index, err error) {
	eventbus.Publish(EvtCloudBeforeDownloadIndex, context, id)
	index = &entity.Index{}

	key := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, "indexes", id)
	data, err := repo.downloadCloudObject(key)
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
	index = &entity.Index{}

	key := path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, "refs", "latest")
	eventbus.Publish(EvtCloudBeforeDownloadRef, context, "refs/latest")
	data, err := repo.downloadCloudObject(key)
	if nil != err {
		if errors.Is(err, ErrCloudObjectNotFound) {
			err = nil
			return
		}
		return
	}
	latestID := string(data)
	key = path.Join("siyuan", cloudInfo.UserID, "repo", cloudInfo.Dir, "indexes", latestID)
	eventbus.Publish(EvtCloudBeforeDownloadIndex, context, latestID)
	data, err = repo.downloadCloudObject(key)
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

func (repo *Repo) CheckoutFilesFromCloud(files []*entity.File, cloudInfo *CloudInfo, context map[string]interface{}) (stat *DownloadTrafficStat, err error) {
	stat = &DownloadTrafficStat{}

	chunkIDs := repo.getChunks(files)
	chunkIDs, err = repo.localNotFoundChunks(chunkIDs)
	if nil != err {
		return
	}

	stat.DownloadBytes, err = repo.downloadCloudChunksPut(chunkIDs, cloudInfo, context)
	if nil != err {
		return
	}
	stat.DownloadChunkCount += len(chunkIDs)

	err = repo.checkoutFiles(files, context)
	return
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// ???????????????????????????

func RemoveCloudRepo(name string, cloudInfo *CloudInfo) (err error) {
	request := httpclient.NewCloudRequest()
	resp, err := request.
		SetBody(map[string]string{"name": name, "token": cloudInfo.Token}).
		Post(cloudInfo.Server + "/apis/siyuan/dejavu/removeRepo")
	if nil != err {
		err = fmt.Errorf("remove cloud repo failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("remove cloud repo failed [%d]", resp.StatusCode)
		return
	}
	return
}

func CreateCloudRepo(name string, cloudInfo *CloudInfo) (err error) {
	result := map[string]interface{}{}
	request := httpclient.NewCloudRequest()
	resp, err := request.
		SetResult(&result).
		SetBody(map[string]string{"name": name, "token": cloudInfo.Token}).
		Post(cloudInfo.Server + "/apis/siyuan/dejavu/createRepo")
	if nil != err {
		err = fmt.Errorf("create cloud repo failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("create cloud repo failed [%d]", resp.StatusCode)
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = fmt.Errorf("create cloud repo failed: %s", result["msg"])
		return
	}
	return
}

func GetCloudRepos(cloudInfo *CloudInfo) (repos []map[string]interface{}, size int64, err error) {
	result := map[string]interface{}{}
	request := httpclient.NewCloudRequest()
	resp, err := request.
		SetBody(map[string]interface{}{"token": cloudInfo.Token}).
		SetResult(&result).
		Post(cloudInfo.Server + "/apis/siyuan/dejavu/getRepos?uid=" + cloudInfo.UserID)
	if nil != err {
		err = fmt.Errorf("get cloud repos failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("request cloud repo list failed [%d]", resp.StatusCode)
		return
	}

	code := result["code"].(float64)
	if 0 != code {
		err = fmt.Errorf("request cloud repo list failed: %s", result["msg"].(string))
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
