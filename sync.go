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
	"strings"
	"sync"
	"time"

	"github.com/88250/gulu"
	"github.com/panjf2000/ants/v2"
	ignore "github.com/sabhiram/go-gitignore"
	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/eventbus"
	"github.com/siyuan-note/filelock"
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

func (mr *MergeResult) DataChanged() bool {
	return len(mr.Upserts) > 0 || len(mr.Removes) > 0 || len(mr.Conflicts) > 0
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

type APITrafficStat struct {
	APIGet int
	APIPut int
}

type TrafficStat struct {
	DownloadTrafficStat
	UploadTrafficStat
	APITrafficStat
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

	// 获取本地最新索引
	latest, err := repo.Latest()
	if nil != err {
		logging.LogErrorf("get latest failed: %s", err)
		return
	}

	// 从云端获取最新索引
	length, cloudLatest, err := repo.downloadCloudLatest(context)
	if nil != err {
		if !errors.Is(err, cloud.ErrCloudObjectNotFound) {
			logging.LogErrorf("download cloud latest failed: %s", err)
			return
		}
	}
	trafficStat.DownloadFileCount++
	trafficStat.DownloadBytes += length
	trafficStat.APIGet++

	if cloudLatest.ID == latest.ID {
		// 数据一致，直接返回
		return
	}

	availableSize := repo.cloud.GetAvailableSize()
	if availableSize <= cloudLatest.Size || availableSize <= latest.Size {
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
	trafficStat.DownloadFileCount += len(fetchFileIDs)
	trafficStat.APIGet += trafficStat.DownloadFileCount

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
	// 组装还原云端最新文件列表
	cloudLatestFiles, err := repo.getFiles(cloudLatest.Files)
	if nil != err {
		logging.LogErrorf("get cloud latest files failed: %s", err)
		return
	}

	// 从文件列表中得到去重后的分块列表
	cloudChunkIDs := repo.getChunks(cloudLatestFiles)

	// 计算本地缺失的分块
	fetchChunkIDs, err := repo.localNotFoundChunks(cloudChunkIDs)
	if nil != err {
		logging.LogErrorf("get local not found chunks failed: %s", err)
		return
	}

	// 锁定云端，防止其他设备并发上传数据
	err = repo.tryLockCloud(context)
	if nil != err {
		return
	}
	defer repo.unlockCloud(context)

	// 上传数据
	err = repo.uploadCloud(context, latest, cloudLatest, cloudChunkIDs, trafficStat)
	if nil != err {
		logging.LogErrorf("upload cloud failed: %s", err)
		return
	}

	// 从云端下载缺失分块并入库
	length, err := repo.downloadCloudChunksPut(fetchChunkIDs, context)
	if nil != err {
		logging.LogErrorf("download cloud chunks put failed: %s", err)
		return
	}
	trafficStat.DownloadBytes += length
	trafficStat.DownloadChunkCount += len(fetchChunkIDs)
	trafficStat.APIGet += trafficStat.DownloadChunkCount

	// 计算本地相比上一个同步点的 upsert 和 remove 差异
	latestFiles, err := repo.getFiles(latest.Files)
	if nil != err {
		logging.LogErrorf("get latest files failed: %s", err)
		return
	}
	latestSync := repo.latestSync()
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

	// 避免旧的本地数据覆盖云端数据 https://github.com/siyuan-note/siyuan/issues/7403
	localUpserts = repo.filterLocalUpserts(localUpserts, cloudUpserts)

	// 记录本地 syncignore 变更
	var localUpsertIgnore *entity.File
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
	var cloudUpsertIgnore *entity.File
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
			if strings.HasSuffix(cloudUpsert.Path, ".tmp") {
				// 数据仓库不迁出 `.tmp` 临时文件 https://github.com/siyuan-note/siyuan/issues/7087
				logging.LogWarnf("ignored tmp file [%s]", cloudUpsert.Path)
				continue
			}
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
		if err = repo.checkoutFile(cloudUpsertIgnore, coDir, 1, 1, context); nil != err {
			logging.LogErrorf("checkout ignore file failed: %s", err)
			return
		}
		data, readErr := filelock.ReadFile(filepath.Join(coDir, cloudUpsertIgnore.Path))
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
	var mergeResultRemovesTmp []*entity.File
	for _, remove := range mergeResult.Removes {
		if !ignoreMatcher.MatchesPath(remove.Path) {
			mergeResultRemovesTmp = append(mergeResultRemovesTmp, remove)
			continue
		}
		// logging.LogInfof("sync merge ignore remove [%s]", remove.Path)
	}
	mergeResult.Removes = mergeResultRemovesTmp

	// 冲突文件复制到数据历史文件夹
	if 0 < len(tmpMergeConflicts) {
		now := mergeResult.Time.Format("2006-01-02-150405")
		temp := filepath.Join(repo.TempPath, "repo", "sync", "conflicts", now)
		for i, file := range tmpMergeConflicts {
			var checkoutTmp *entity.File
			checkoutTmp, err = repo.store.GetFile(file.ID)
			if nil != err {
				logging.LogErrorf("get file failed: %s", err)
				return
			}

			err = repo.checkoutFile(checkoutTmp, temp, i+1, len(tmpMergeConflicts), context)
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
		err = repo.store.PutIndex(latest)
		if nil != err {
			logging.LogErrorf("put merge index failed: %s", err)
			return
		}

		// 索引后的 upserts 需要上传到云端
		err = repo.uploadCloud(context, latest, cloudLatest, cloudChunkIDs, trafficStat)
		if nil != err {
			logging.LogErrorf("upload cloud failed: %s", err)
			return
		}
	}

	// 上传索引
	length, err = repo.uploadIndex(latest, context)
	if nil != err {
		logging.LogErrorf("upload indexes failed: %s", err)
		return
	}
	trafficStat.UploadFileCount++
	trafficStat.UploadBytes += length
	trafficStat.APIPut++

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
	trafficStat.UploadFileCount++
	trafficStat.UploadBytes += length
	trafficStat.APIPut++

	// 更新云端索引列表
	downloadBytes, uploadBytes, err := repo.updateCloudIndexes(latest, context)
	if nil != err {
		logging.LogErrorf("update cloud indexes failed: %s", err)
		return
	}
	trafficStat.DownloadBytes += downloadBytes
	trafficStat.UploadBytes += uploadBytes
	trafficStat.APIGet++
	trafficStat.APIPut++

	// 更新本地同步点
	err = repo.UpdateLatestSync(latest.ID)
	if nil != err {
		logging.LogErrorf("update latest sync failed: %s", err)
		return
	}

	// 统计流量
	go repo.cloud.AddTraffic(&cloud.Traffic{
		UploadBytes:   trafficStat.UploadBytes,
		DownloadBytes: trafficStat.DownloadBytes,
		APIGet:        trafficStat.APIGet,
		APIPut:        trafficStat.APIPut,
	})

	// 移除空目录
	err = gulu.File.RemoveEmptyDirs(repo.DataPath, removeEmptyDirExcludes...)
	if nil != err {
		logging.LogErrorf("remove empty dirs failed: %s", err)
		return
	}
	return
}

// filterLocalUpserts 避免旧的本地数据覆盖云端数据 https://github.com/siyuan-note/siyuan/issues/7403
func (repo *Repo) filterLocalUpserts(localUpserts, cloudUpserts []*entity.File) (ret []*entity.File) {
	cloudUpsertsMap := map[string]*entity.File{}
	for _, cloudUpsert := range cloudUpserts {
		cloudUpsertsMap[cloudUpsert.Path] = cloudUpsert
	}

	var toRemoveLocalUpsertPaths []string
	for _, localUpsert := range localUpserts {
		if cloudUpsert := cloudUpsertsMap[localUpsert.Path]; nil != cloudUpsert {
			if localUpsert.Updated < cloudUpsert.Updated-1000*60*7 { // 本地早于云端 7 分钟
				toRemoveLocalUpsertPaths = append(toRemoveLocalUpsertPaths, localUpsert.Path) // 使用云端数据覆盖本地数据
			}
		}
	}

	for _, localUpsert := range localUpserts {
		if !gulu.Str.Contains(localUpsert.Path, toRemoveLocalUpsertPaths) {
			ret = append(ret, localUpsert)
		}
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
		if !errors.Is(err, cloud.ErrCloudObjectNotFound) {
			logging.LogErrorf("download cloud latest failed: %s", err)
			return
		}
	}
	trafficStat := &TrafficStat{}
	trafficStat.DownloadFileCount++
	trafficStat.DownloadBytes += length
	trafficStat.APIGet++

	if cloudLatest.ID == latest.ID {
		// 数据一致，直接返回
		return
	}

	availableSize := repo.cloud.GetAvailableSize()
	if availableSize <= cloudLatest.Size || availableSize <= latest.Size {
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
	trafficStat.DownloadFileCount += len(fetchFileIDs)
	trafficStat.APIGet += len(fetchFileIDs)

	// 统计流量
	go repo.cloud.AddTraffic(&cloud.Traffic{
		UploadBytes:   trafficStat.UploadBytes,
		DownloadBytes: trafficStat.DownloadBytes,
		APIGet:        trafficStat.APIGet,
	})
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
	count, total := 0, len(chunkIDs)
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != downloadErr {
			return // 快速失败
		}

		chunkID := arg.(string)
		count++
		length, chunk, dccErr := repo.downloadCloudChunk(chunkID, count, total, context)
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

	eventbus.Publish(eventbus.EvtCloudBeforeDownloadChunks, context, total)
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
	count, total := 0, len(fileIDs)
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != downloadErr {
			return // 快速失败
		}

		fileID := arg.(string)
		count++
		length, file, dcfErr := repo.downloadCloudFile(fileID, count, total, context)
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

	eventbus.Publish(eventbus.EvtCloudBeforeDownloadFiles, context, total)
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
	total := len(files)
	eventbus.Publish(eventbus.EvtCheckoutRemoveFiles, context, total)
	for i, file := range files {
		absPath := repo.absPath(file.Path)
		if err = filelock.Remove(absPath); nil != err {
			return
		}
		eventbus.Publish(eventbus.EvtCheckoutRemoveFile, context, i+1, total)
	}
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

func (repo *Repo) updateCloudRef(ref string, context map[string]interface{}) (uploadBytes int64, err error) {
	eventbus.Publish(eventbus.EvtCloudBeforeUploadRef, context, ref)

	absFilePath := filepath.Join(repo.Path, ref)
	info, err := os.Stat(absFilePath)
	if nil != err {
		return
	}
	uploadBytes = info.Size()
	err = repo.cloud.UploadObject(ref, true)
	return
}

func (repo *Repo) updateCloudIndexes(latest *entity.Index, context map[string]interface{}) (downloadBytes, uploadBytes int64, err error) {
	data, err := repo.cloud.DownloadObject("indexes-v2.json")
	if nil != err {
		if !errors.Is(err, cloud.ErrCloudObjectNotFound) {
			return
		}
		err = nil
	}
	downloadBytes = int64(len(data))

	data, err = repo.store.compressDecoder.DecodeAll(data, nil)
	if nil != err {
		return
	}

	indexes := &cloud.Indexes{}
	if 0 < len(data) {
		if err = gulu.JSON.UnmarshalJSON(data, &indexes); nil != err {
			logging.LogWarnf("unmarshal cloud indexes-v2.json failed: %s", err)
		}

		// Deduplication when uploading cloud snapshot indexes https://github.com/siyuan-note/siyuan/issues/8424
		found := false
		tmp := &cloud.Indexes{}
		added := map[string]bool{}
		for _, index := range indexes.Indexes {
			if index.ID == latest.ID {
				found = true
			}

			if !added[index.ID] {
				tmp.Indexes = append(tmp.Indexes, index)
				added[index.ID] = true
			}
		}
		if found {
			return
		}
		indexes = tmp
	}

	indexes.Indexes = append([]*cloud.Index{
		{
			ID:         latest.ID,
			SystemID:   latest.SystemID,
			SystemName: latest.SystemName,
			SystemOS:   latest.SystemOS,
		},
	}, indexes.Indexes...)
	if data, err = gulu.JSON.MarshalIndentJSON(indexes, "", "\t"); nil != err {
		return
	}

	data = repo.store.compressEncoder.EncodeAll(data, nil)
	uploadBytes = int64(len(data))

	if err = gulu.File.WriteFileSafer(filepath.Join(repo.Path, "indexes-v2.json"), data, 0644); nil != err {
		return
	}

	err = repo.cloud.UploadObject("indexes-v2.json", true)
	return
}

func (repo *Repo) uploadIndex(index *entity.Index, context map[string]interface{}) (uploadBytes int64, err error) {
	absFilePath := filepath.Join(repo.Path, "indexes", index.ID)
	info, statErr := os.Stat(absFilePath)
	if nil != statErr {
		return
	}
	length := info.Size()
	uploadBytes += length

	eventbus.Publish(eventbus.EvtCloudBeforeUploadIndex, context, index.ID)
	err = repo.cloud.UploadObject(path.Join("indexes", index.ID), false)
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
	count, total := 0, len(upsertFiles)
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != uploadErr {
			return // 快速失败
		}

		upsertFileID := arg.(string)
		filePath := path.Join("objects", upsertFileID[:2], upsertFileID[2:])
		count++
		eventbus.Publish(eventbus.EvtCloudBeforeUploadFile, context, count, total)
		if uoErr := repo.cloud.UploadObject(filePath, false); nil != uoErr {
			uploadErr = uoErr
			return
		}
	})
	if nil != err {
		return
	}

	eventbus.Publish(eventbus.EvtCloudBeforeUploadFiles, context, total)
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
	count, total := 0, len(upsertChunkIDs)
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != uploadErr {
			return // 快速失败
		}

		upsertChunkID := arg.(string)
		filePath := path.Join("objects", upsertChunkID[:2], upsertChunkID[2:])
		count++
		eventbus.Publish(eventbus.EvtCloudBeforeUploadChunk, context, count, total)
		if uoErr := repo.cloud.UploadObject(filePath, false); nil != uoErr {
			uploadErr = uoErr
			return
		}
	})
	if nil != err {
		return
	}

	eventbus.Publish(eventbus.EvtCloudBeforeUploadChunks, context, total)
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

func (repo *Repo) localUpsertFiles(latest *entity.Index, cloudLatest *entity.Index) (ret []*entity.File, err error) {
	files := map[string]bool{}
	for _, file := range latest.Files {
		files[file] = true
	}

	for _, cloudFileID := range cloudLatest.Files {
		delete(files, cloudFileID)
	}

	for fileID := range files {
		file, getErr := repo.store.GetFile(fileID)
		if nil != getErr {
			logging.LogErrorf("get file [%s] failed: %s", fileID, getErr)
			return
		}
		if nil == file {
			logging.LogErrorf("file [%s] not found", fileID)
			err = ErrNotFoundObject
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

func (repo *Repo) uploadCloud(context map[string]interface{},
	latest, cloudLatest *entity.Index, cloudChunkIDs []string, trafficStat *TrafficStat) (err error) {
	// 计算待上传云端的本地变更文件
	upsertFiles, err := repo.localUpsertFiles(latest, cloudLatest)
	if nil != err {
		logging.LogErrorf("get local upsert files failed: %s", err)
		return
	}

	if 1 > len(upsertFiles) {
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
	trafficStat.UploadChunkCount += len(upsertChunkIDs)
	trafficStat.UploadBytes += length
	trafficStat.APIPut += trafficStat.UploadChunkCount

	// 上传文件
	length, err = repo.uploadFiles(upsertFiles, context)
	if nil != err {
		logging.LogErrorf("upload files failed: %s", err)
		return
	}
	trafficStat.UploadFileCount += len(upsertFiles)
	trafficStat.UploadBytes += length
	trafficStat.APIPut += trafficStat.UploadFileCount
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

func (repo *Repo) downloadCloudChunk(id string, count, total int, context map[string]interface{}) (length int64, ret *entity.Chunk, err error) {
	eventbus.Publish(eventbus.EvtCloudBeforeDownloadChunk, context, count, total)

	key := path.Join("objects", id[:2], id[2:])
	data, err := repo.downloadCloudObject(key)
	if nil != err {
		logging.LogErrorf("download cloud chunk [%s] failed: %s", id, err)
		return
	}
	length = int64(len(data))
	ret = &entity.Chunk{ID: id, Data: data}
	return
}

func (repo *Repo) downloadCloudFile(id string, count, total int, context map[string]interface{}) (length int64, ret *entity.File, err error) {
	eventbus.Publish(eventbus.EvtCloudBeforeDownloadFile, context, count, total)

	key := path.Join("objects", id[:2], id[2:])
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

func (repo *Repo) downloadCloudObject(filePath string) (ret []byte, err error) {
	data, err := repo.cloud.DownloadObject(filePath)
	if nil != err {
		return
	}

	ret, err = repo.decodeDownloadedData(filePath, data)
	if nil != err {
		return
	}
	//logging.LogInfof("downloaded object [%s]", filePath)
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

	key := path.Join("indexes", id)
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

	key := path.Join("refs", "latest")
	eventbus.Publish(eventbus.EvtCloudBeforeDownloadRef, context, "refs/latest")
	data, err := repo.downloadCloudObject(key)
	if nil != err {
		if errors.Is(err, cloud.ErrCloudObjectNotFound) {
			err = nil
			return
		}
		return
	}
	latestID := string(data)
	key = path.Join("indexes", latestID)
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

func (repo *Repo) RemoveCloudRepo(name string) (err error) {
	return repo.cloud.RemoveRepo(name)
}

func (repo *Repo) CreateCloudRepo(name string) (err error) {
	return repo.cloud.CreateRepo(name)
}

func (repo *Repo) GetCloudRepos() (repos []*cloud.Repo, size int64, err error) {
	return repo.cloud.GetRepos()
}

func (repo *Repo) GetCloudAvailableSize() (ret int64) {
	return repo.cloud.GetAvailableSize()
}

func (repo *Repo) GetCloudRepoStat() (stat *cloud.Stat, err error) {
	return repo.cloud.GetStat()
}
