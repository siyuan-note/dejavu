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
	ignore "github.com/sabhiram/go-gitignore"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/transport"
	"github.com/siyuan-note/eventbus"
	"github.com/siyuan-note/filelock"
	"github.com/siyuan-note/httpclient"
	"github.com/siyuan-note/logging"
)

var (
	ErrCloudStorageSizeExceeded = errors.New("cloud storage limit size exceeded")
	ErrCloudBackupCountExceeded = errors.New("cloud backup count exceeded")

	ErrCloudGenerateConflictHistory = errors.New("generate conflict history failed")
)

type MergeResult struct {
	Time                        time.Time
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

func (repo *Repo) GetSyncCloudFiles(context map[string]interface{}) (fetchedFiles []*entity.File, err error) {
	lock.Lock()
	defer lock.Unlock()

	fetchedFiles, err = repo.getSyncCloudFiles(context)
	return
}

func (repo *Repo) Sync(context map[string]interface{}) (mergeResult *MergeResult, trafficStat *TrafficStat, err error) {
	lock.Lock()
	defer lock.Unlock()

	mergeResult, trafficStat, err = repo.sync(context)
	if e, ok := err.(*os.PathError); ok && os.IsNotExist(err) {
		p := e.Path
		if !strings.Contains(p, "objects") {
			return
		}

		// 索引时正常，但是上传时可能因为外部变更导致对象（文件或者分块）不存在，此时需要告知用户数据仓库已经损坏，需要重置数据仓库
		logging.LogErrorf("sync failed: %s", err)
		err = ErrRepoFatalErr
	}
	return
}

func (repo *Repo) sync(context map[string]interface{}) (mergeResult *MergeResult, trafficStat *TrafficStat, err error) {
	mergeResult = &MergeResult{Time: time.Now()}
	trafficStat = &TrafficStat{}

	latest, err := repo.Latest()
	if nil != err {
		logging.LogErrorf("get latest failed: %s", err)
		return
	}

	// 从云端获取最新索引
	length, cloudLatest, err := repo.downloadCloudLatest(context)
	if nil != err {
		if !errors.Is(err, transport.ErrCloudObjectNotFound) {
			logging.LogErrorf("download cloud latest failed: %s", err)
			return
		}
	}
	trafficStat.DownloadFileCount++
	trafficStat.DownloadBytes += length

	if cloudLatest.ID == latest.ID {
		// 数据一致，直接返回
		return
	}

	limitSize := repo.GetCloudLimitSize()
	if limitSize <= cloudLatest.Size || limitSize <= latest.Size {
		err = ErrCloudStorageSizeExceeded
		return
	}

	// 计算本地缺失的文件
	fetchFileIDs, err := repo.localNotFoundFiles(cloudLatest.Files)
	if nil != err {
		logging.LogErrorf("get local not found files failed: %s", err)
		return
	}

	// 从云端下载缺失文件并入库
	length, fetchedFiles, err := repo.downloadCloudFilesPut(fetchFileIDs, context)
	if nil != err {
		logging.LogErrorf("download cloud files put failed: %s", err)
		return
	}
	trafficStat.DownloadBytes += length
	trafficStat.DownloadFileCount = len(fetchFileIDs)

	// 执行数据同步
	err = repo.sync0(context, fetchedFiles, cloudLatest, latest, mergeResult, trafficStat)
	return
}

// sync0 实现了数据同步的核心逻辑。
//
// fetchedFiles 已从云端下载的文件
// cloudLatest 云端最新索引
// latest 本地最新索引
// mergeResult 待返回的同步合并结果
// trafficStat 待返回的流量统计
func (repo *Repo) sync0(context map[string]interface{},
	fetchedFiles []*entity.File, cloudLatest *entity.Index, latest *entity.Index, mergeResult *MergeResult, trafficStat *TrafficStat) (err error) {
	// 获取本地同步点到最新索引之间的索引列表
	latestSync := repo.latestSync()
	localIndexes := repo.getIndexes(latest.ID, latestSync.ID)

	// 从文件列表中得到去重后的分块列表
	cloudChunkIDs := repo.getChunks(fetchedFiles)

	// 计算本地缺失的分块
	fetchChunkIDs, err := repo.localNotFoundChunks(cloudChunkIDs)
	if nil != err {
		logging.LogErrorf("get local not found chunks failed: %s", err)
		return
	}

	// 计算待上传云端的本地变更文件
	upsertFiles, err := repo.localUpsertFiles(localIndexes, cloudLatest.Files)
	if nil != err {
		logging.LogErrorf("get local upsert files failed: %s", err)
		return
	}

	// 计算待上传云端的分块
	upsertChunkIDs, err := repo.localUpsertChunkIDs(upsertFiles, cloudChunkIDs)
	if nil != err {
		logging.LogErrorf("get local upsert chunk ids failed: %s", err)
		return
	}

	// 上传分块
	length, err := repo.uploadChunks(upsertChunkIDs, context)
	if nil != err {
		logging.LogErrorf("upload chunks failed: %s", err)
		return
	}
	trafficStat.UploadChunkCount = len(upsertChunkIDs)
	trafficStat.UploadBytes += length

	// 上传文件
	length, err = repo.uploadFiles(upsertFiles, context)
	if nil != err {
		logging.LogErrorf("upload files failed: %s", err)
		return
	}
	trafficStat.UploadFileCount = len(upsertFiles)
	trafficStat.UploadBytes += length

	// 从云端下载缺失分块并入库
	length, err = repo.downloadCloudChunksPut(fetchChunkIDs, context)
	if nil != err {
		logging.LogErrorf("download cloud chunks put failed: %s", err)
		return
	}
	trafficStat.DownloadBytes += length
	trafficStat.DownloadChunkCount = len(fetchChunkIDs)

	// 组装还原云端最新文件列表
	cloudLatestFiles, err := repo.getFiles(cloudLatest.Files)
	if nil != err {
		logging.LogErrorf("get cloud latest files failed: %s", err)
		return
	}

	// 校验本地缺失的分块，如果不全则下载补全
	//
	// 因为前面通过 fetchedFiles 下载文件成功后下载分块可能失败，导致文件对象并不完整，后续再次重试时 fetchedFiles 就不会再有待获取文件
	// 所以这里需要根据还原出来的云端最新文件列表中再次校验缺失的块并下载补全
	cloudChunkIDs = repo.getChunks(cloudLatestFiles)
	fetchChunkIDs, err = repo.localNotFoundChunks(cloudChunkIDs)
	if nil != err {
		logging.LogErrorf("get local not found chunks failed: %s", err)
		return
	}
	length, err = repo.downloadCloudChunksPut(fetchChunkIDs, context)
	if nil != err {
		logging.LogErrorf("download cloud chunks put failed: %s", err)
		return
	}
	trafficStat.DownloadBytes += length
	trafficStat.DownloadChunkCount = len(fetchChunkIDs)

	// 计算本地相比上一个同步点的 upsert 和 remove 差异
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

	// 计算云端最新相比本地最新的 upsert 和 remove 差异
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

	var fetchedFileIDs []string
	for _, fetchedFile := range fetchedFiles {
		fetchedFileIDs = append(fetchedFileIDs, fetchedFile.ID)
	}

	// 计算冲突的 upsert 和无冲突能够合并的 upsert
	// 冲突的文件以本地 upsert 和 remove 为准
	var tmpMergeConflicts []*entity.File
	for _, cloudUpsert := range cloudUpserts {
		if "/.siyuan/syncignore" == cloudUpsert.Path {
			cloudUpsertIgnore = cloudUpsert
		}

		if repo.existDataFile(localUpserts, cloudUpsert) {
			// 无论是否发生实际下载文件，都需要生成本地历史，以确保任何情况下都能够通过数据历史恢复文件
			tmpMergeConflicts = append(tmpMergeConflicts, cloudUpsert)

			if gulu.Str.Contains(cloudUpsert.ID, fetchedFileIDs) {
				// 发生实际下载文件的情况下才能认为云端有更新的 upsert 从而导致了冲突
				// 冲突列表在外部单独处理生成副本
				mergeResult.Conflicts = append(mergeResult.Conflicts, cloudUpsert)
			}
			continue
		}

		if !repo.existDataFile(localRemoves, cloudUpsert) {
			mergeResult.Upserts = append(mergeResult.Upserts, cloudUpsert)
		}
	}

	// 计算能够无冲突合并的 remove，冲突的文件以本地 upsert 为准
	for _, cloudRemove := range cloudRemoves {
		if !repo.existDataFile(localUpserts, cloudRemove) {
			mergeResult.Removes = append(mergeResult.Removes, cloudRemove)
		}
	}

	// 云端如果更新了忽略文件则使用其规则过滤 remove，避免后面误删本地文件 https://github.com/siyuan-note/siyuan/issues/5497
	var ignoreLines []string
	if nil != cloudUpsertIgnore {
		coDir := filepath.Join(repo.DataPath)
		if nil != localUpsertIgnore {
			// 本地 syncignore 存在变更，则临时迁出
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

	// 冲突文件复制到数据历史文件夹
	if 0 < len(tmpMergeConflicts) {
		now := mergeResult.Time.Format("2006-01-02-150405")
		temp := filepath.Join(repo.TempPath, "repo", "sync", "conflicts", now)
		for _, file := range tmpMergeConflicts {
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

	// 数据变更后需要还原工作区并创建 merge 快照
	if 0 < len(mergeResult.Upserts) || 0 < len(mergeResult.Removes) {
		if 0 < len(mergeResult.Upserts) {
			// 迁出到工作区
			err = repo.checkoutFiles(mergeResult.Upserts, context)
			if nil != err {
				logging.LogErrorf("checkout files failed: %s", err)
				return
			}
		}

		if 0 < len(mergeResult.Removes) {
			// 删除工作区文件
			err = repo.removeFiles(mergeResult.Removes, context)
			if nil != err {
				logging.LogErrorf("remove files failed: %s", err)
				return
			}
		}

		// 创建 merge 快照
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

		// 索引后的 upserts 需要上传到云端
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

		length, err = repo.uploadChunks(upsertChunkIDs, context)
		if nil != err {
			logging.LogErrorf("upload chunks failed: %s", err)
			return
		}
		trafficStat.UploadChunkCount = len(upsertChunkIDs)
		trafficStat.UploadBytes += length

		length, err = repo.uploadFiles(upsertFiles, context)
		if nil != err {
			logging.LogErrorf("upload files failed: %s", err)
			return
		}
		trafficStat.UploadFileCount = len(upsertFiles)
		trafficStat.UploadBytes += length
	}

	// 上传索引
	length, err = repo.uploadIndexes(localIndexes, context)
	if nil != err {
		logging.LogErrorf("upload indexes failed: %s", err)
		return
	}
	trafficStat.UploadBytes += length

	// 更新本地 latest
	err = repo.UpdateLatest(latest.ID)
	if nil != err {
		logging.LogErrorf("update latest failed: %s", err)
		return
	}

	// 更新云端 latest
	length, err = repo.updateCloudRef("refs/latest", context)
	if nil != err {
		logging.LogErrorf("update cloud [refs/latest] failed: %s", err)
		return
	}
	trafficStat.UploadBytes += length

	// 更新本地同步点
	err = repo.UpdateLatestSync(latest.ID)
	if nil != err {
		logging.LogErrorf("update latest sync failed: %s", err)
		return
	}

	// 统计流量
	go repo.addTraffic(trafficStat.UploadBytes, trafficStat.DownloadBytes)

	// 移除空目录
	err = gulu.File.RemoveEmptyDirs(repo.DataPath, workspaceDataDirs...)
	if nil != err {
		logging.LogErrorf("remove empty dirs failed: %s", err)
		return
	}
	return
}

func (repo *Repo) getSyncCloudFiles(context map[string]interface{}) (fetchedFiles []*entity.File, err error) {
	latest, err := repo.Latest()
	if nil != err {
		logging.LogErrorf("get latest failed: %s", err)
		return
	}

	// 从云端获取最新索引
	length, cloudLatest, err := repo.downloadCloudLatest(context)
	if nil != err {
		if !errors.Is(err, transport.ErrCloudObjectNotFound) {
			logging.LogErrorf("download cloud latest failed: %s", err)
			return
		}
	}
	trafficStat := &TrafficStat{}
	trafficStat.DownloadFileCount++
	trafficStat.DownloadBytes += length

	if cloudLatest.ID == latest.ID {
		// 数据一致，直接返回
		return
	}

	limitSize := repo.GetCloudLimitSize()
	if limitSize <= cloudLatest.Size || limitSize <= latest.Size {
		err = ErrCloudStorageSizeExceeded
		return
	}

	// 计算本地缺失的文件
	fetchFileIDs, err := repo.localNotFoundFiles(cloudLatest.Files)
	if nil != err {
		logging.LogErrorf("get local not found files failed: %s", err)
		return
	}

	// 从云端下载缺失文件并入库
	length, fetchedFiles, err = repo.downloadCloudFilesPut(fetchFileIDs, context)
	if nil != err {
		logging.LogErrorf("download cloud files put failed: %s", err)
		return
	}
	trafficStat.DownloadBytes += length
	trafficStat.DownloadFileCount = len(fetchFileIDs)

	// 统计流量
	go repo.addTraffic(trafficStat.UploadBytes, trafficStat.DownloadBytes)
	return
}

func (repo *Repo) downloadCloudChunksPut(chunkIDs []string, context map[string]interface{}) (downloadBytes int64, err error) {
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
			return // 快速失败
		}

		chunkID := arg.(string)
		length, chunk, dccErr := repo.downloadCloudChunk(chunkID, context)
		if nil != dccErr {
			downloadErr = dccErr
			return
		}
		if pcErr := repo.store.PutChunk(chunk); nil != pcErr {
			downloadErr = pcErr
			return
		}
		downloadBytes += length
	})
	if nil != err {
		return
	}

	eventbus.Publish(eventbus.EvtCloudBeforeDownloadChunks, context, chunkIDs)
	for _, chunkID := range chunkIDs {
		waitGroup.Add(1)
		if err = p.Invoke(chunkID); nil != err {
			return
		}
		if nil != downloadErr {
			err = downloadErr
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

func (repo *Repo) downloadCloudFilesPut(fileIDs []string, context map[string]interface{}) (downloadBytes int64, ret []*entity.File, err error) {
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
			return // 快速失败
		}

		fileID := arg.(string)
		length, file, dcfErr := repo.downloadCloudFile(fileID, context)
		if nil != dcfErr {
			downloadErr = dcfErr
			return
		}
		if pfErr := repo.store.PutFile(file); nil != pfErr {
			downloadErr = pfErr
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

	eventbus.Publish(eventbus.EvtCloudBeforeDownloadFiles, context, fileIDs)
	for _, fileID := range fileIDs {
		waitGroup.Add(1)
		if err = p.Invoke(fileID); nil != err {
			return
		}
		if nil != downloadErr {
			err = downloadErr
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
	eventbus.Publish(eventbus.EvtCheckoutRemoveFiles, context, files)
	for _, file := range files {
		absPath := repo.absPath(file.Path)
		if err = filelock.Remove(absPath); nil != err {
			return
		}
		eventbus.Publish(eventbus.EvtCheckoutRemoveFile, context, file.Path)
	}
	return
}

func (repo *Repo) checkoutFiles(files []*entity.File, context map[string]interface{}) (err error) {
	eventbus.Publish(eventbus.EvtCheckoutUpsertFiles, context, files)
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

	if err = filelock.WriteFile(absPath, data); nil != err {
		return
	}

	updated := time.UnixMilli(file.Updated)
	if err = os.Chtimes(absPath, updated, updated); nil != err {
		logging.LogErrorf("change [%s] time failed: %s", absPath, err)
		return
	}
	eventbus.Publish(eventbus.EvtCheckoutUpsertFile, context, file.Path)
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

func (repo *Repo) updateCloudRef(ref string, context map[string]interface{}) (uploadBytes int64, err error) {
	eventbus.Publish(eventbus.EvtCloudBeforeUploadRef, context, ref)

	absFilePath := filepath.Join(repo.Path, ref)
	info, err := os.Stat(absFilePath)
	if nil != err {
		return
	}
	uploadBytes = info.Size()
	err = repo.transport.UploadObject(ref, true)
	return
}

func (repo *Repo) uploadIndexes(indexes []*entity.Index, context map[string]interface{}) (uploadBytes int64, err error) {
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
		eventbus.Publish(eventbus.EvtCloudBeforeUploadIndex, context, indexID)
		if uoErr := repo.transport.UploadObject(path.Join("indexes", indexID), false); nil != uoErr {
			uploadErr = uoErr
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
		if nil != uploadErr {
			err = uploadErr
			return
		}
	}
	waitGroup.Wait()
	p.Release()
	return
}

func (repo *Repo) uploadFiles(upsertFiles []*entity.File, context map[string]interface{}) (uploadBytes int64, err error) {
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
	var uploadErr error
	poolSize := 8
	if poolSize > len(upsertFiles) {
		poolSize = len(upsertFiles)
	}
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != uploadErr {
			return // 快速失败
		}

		upsertFileID := arg.(string)
		filePath := path.Join("objects", upsertFileID[:2], upsertFileID[2:])
		eventbus.Publish(eventbus.EvtCloudBeforeUploadFile, context, upsertFileID)
		if uoErr := repo.transport.UploadObject(filePath, false); nil != uoErr {
			uploadErr = uoErr
			return
		}
	})
	if nil != err {
		return
	}

	eventbus.Publish(eventbus.EvtCloudBeforeUploadFiles, context, upsertFiles)
	for _, upsertFile := range upsertFiles {
		waitGroup.Add(1)
		if err = p.Invoke(upsertFile.ID); nil != err {
			return
		}
		if nil != uploadErr {
			err = uploadErr
			return
		}
	}
	waitGroup.Wait()
	p.Release()
	return
}

func (repo *Repo) uploadChunks(upsertChunkIDs []string, context map[string]interface{}) (uploadBytes int64, err error) {
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
	var uploadErr error
	poolSize := 8
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
		eventbus.Publish(eventbus.EvtCloudBeforeUploadChunk, context, upsertChunkID)
		if uoErr := repo.transport.UploadObject(filePath, false); nil != uoErr {
			uploadErr = uoErr
			return
		}
	})
	if nil != err {
		return
	}

	eventbus.Publish(eventbus.EvtCloudBeforeUploadChunks, context, upsertChunkIDs)
	for _, upsertChunkID := range upsertChunkIDs {
		waitGroup.Add(1)
		if err = p.Invoke(upsertChunkID); nil != err {
			return
		}
		if nil != uploadErr {
			err = uploadErr
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

func (repo *Repo) latestSync() (ret *entity.Index) {
	ret = &entity.Index{} // 构造一个空的索引表示没有同步点

	latestSync := filepath.Join(repo.Path, "refs", "latest-sync")
	if !gulu.File.IsExist(latestSync) {
		return
	}

	data, err := os.ReadFile(latestSync)
	if nil != err {
		logging.LogWarnf("read latest sync index failed: %s", err)
		return
	}
	hash := string(data)
	hash = strings.TrimSpace(hash)
	if "" == hash {
		logging.LogWarnf("read latest sync index hash is empty")
		return
	}

	ret, err = repo.store.GetIndex(hash)
	if nil != err {
		logging.LogWarnf("get latest sync index failed: %s", err)
		return
	}
	return
}

func (repo *Repo) addTraffic(uploadBytes, downloadBytes int64) {
	token := repo.transport.GetConf().Token
	server := repo.transport.GetConf().Server

	request := httpclient.NewCloudRequest()
	resp, err := request.
		SetBody(map[string]interface{}{"token": token, "uploadBytes": uploadBytes, "downloadBytes": downloadBytes}).
		Post(server + "/apis/siyuan/dejavu/addTraffic")
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

func (repo *Repo) downloadCloudChunk(id string, context map[string]interface{}) (length int64, ret *entity.Chunk, err error) {
	eventbus.Publish(eventbus.EvtCloudBeforeDownloadChunk, context, id)

	userId := repo.transport.GetConf().UserID
	dir := repo.transport.GetConf().Dir
	key := path.Join("siyuan", userId, "repo", dir, "objects", id[:2], id[2:])
	data, err := repo.downloadCloudObject(key)
	if nil != err {
		logging.LogErrorf("download cloud chunk [%s] failed: %s", id, err)
		return
	}
	length = int64(len(data))
	ret = &entity.Chunk{ID: id, Data: data}
	return
}

func (repo *Repo) downloadCloudFile(id string, context map[string]interface{}) (length int64, ret *entity.File, err error) {
	eventbus.Publish(eventbus.EvtCloudBeforeDownloadFile, context, id)

	userId := repo.transport.GetConf().UserID
	dir := repo.transport.GetConf().Dir
	key := path.Join("siyuan", userId, "repo", dir, "objects", id[:2], id[2:])
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
				logging.LogErrorf("download object [%s] failed: %s", key, transport.ErrCloudObjectNotFound)
			}
			err = transport.ErrCloudObjectNotFound
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

	ret, err = repo.decodeDownloadedData(key, ret)
	if nil != err {
		return
	}
	//logging.LogInfof("downloaded object [%s]", key)
	return
}

func (repo *Repo) decodeDownloadedData(key string, data []byte) (ret []byte, err error) {
	ret = data
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
	return
}

func (repo *Repo) downloadCloudIndex(id string, context map[string]interface{}) (downloadBytes int64, index *entity.Index, err error) {
	eventbus.Publish(eventbus.EvtCloudBeforeDownloadIndex, context, id)
	index = &entity.Index{}

	userId := repo.transport.GetConf().UserID
	dir := repo.transport.GetConf().Dir
	key := path.Join("siyuan", userId, "repo", dir, "indexes", id)
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

func (repo *Repo) downloadCloudLatest(context map[string]interface{}) (downloadBytes int64, index *entity.Index, err error) {
	index = &entity.Index{}

	userId := repo.transport.GetConf().UserID
	dir := repo.transport.GetConf().Dir
	key := path.Join("siyuan", userId, "repo", dir, "refs", "latest")
	eventbus.Publish(eventbus.EvtCloudBeforeDownloadRef, context, "refs/latest")
	data, err := repo.downloadCloudObject(key)
	if nil != err {
		if errors.Is(err, transport.ErrCloudObjectNotFound) {
			err = nil
			return
		}
		return
	}
	latestID := string(data)
	key = path.Join("siyuan", userId, "repo", dir, "indexes", latestID)
	eventbus.Publish(eventbus.EvtCloudBeforeDownloadIndex, context, latestID)
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

func (repo *Repo) CheckoutFilesFromCloud(files []*entity.File, context map[string]interface{}) (stat *DownloadTrafficStat, err error) {
	stat = &DownloadTrafficStat{}

	chunkIDs := repo.getChunks(files)
	chunkIDs, err = repo.localNotFoundChunks(chunkIDs)
	if nil != err {
		return
	}

	stat.DownloadBytes, err = repo.downloadCloudChunksPut(chunkIDs, context)
	if nil != err {
		return
	}
	stat.DownloadChunkCount += len(chunkIDs)

	err = repo.checkoutFiles(files, context)
	return
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 以下是仓库管理接口

func (repo *Repo) RemoveCloudRepo(name string) (err error) {
	token := repo.transport.GetConf().Token
	server := repo.transport.GetConf().Server

	request := httpclient.NewCloudFileRequest15s()
	resp, err := request.
		SetBody(map[string]string{"name": name, "token": token}).
		Post(server + "/apis/siyuan/dejavu/removeRepo")
	if nil != err {
		err = fmt.Errorf("remove cloud repo failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = transport.ErrCloudAuthFailed
			return
		}
		err = fmt.Errorf("remove cloud repo failed [%d]", resp.StatusCode)
		return
	}
	return
}

func (repo *Repo) CreateCloudRepo(name string) (err error) {
	token := repo.transport.GetConf().Token
	server := repo.transport.GetConf().Server

	result := map[string]interface{}{}
	request := httpclient.NewCloudRequest()
	resp, err := request.
		SetResult(&result).
		SetBody(map[string]string{"name": name, "token": token}).
		Post(server + "/apis/siyuan/dejavu/createRepo")
	if nil != err {
		err = fmt.Errorf("create cloud repo failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = transport.ErrCloudAuthFailed
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

func (repo *Repo) GetCloudRepos() (repos []map[string]interface{}, size int64, err error) {
	token := repo.transport.GetConf().Token
	server := repo.transport.GetConf().Server
	userId := repo.transport.GetConf().UserID

	result := map[string]interface{}{}
	request := httpclient.NewCloudRequest()
	resp, err := request.
		SetBody(map[string]interface{}{"token": token}).
		SetResult(&result).
		Post(server + "/apis/siyuan/dejavu/getRepos?uid=" + userId)
	if nil != err {
		err = fmt.Errorf("get cloud repos failed: %s", err)
		return
	}

	if 200 != resp.StatusCode {
		if 401 == resp.StatusCode {
			err = transport.ErrCloudAuthFailed
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

func (repo *Repo) GetCloudLimitSize() (ret int64) {
	ret = repo.transport.GetConf().LimitSize
	if 1 > ret {
		ret = 1024 * 1024 * 1024 * 1024 // 1T
	}
	return
}
