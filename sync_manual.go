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
	"path/filepath"
	"sync"
	"time"

	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/logging"
)

func (repo *Repo) SyncDownload(context map[string]interface{}) (mergeResult *MergeResult, trafficStat *TrafficStat, err error) {
	lock.Lock()
	defer lock.Unlock()
	if err = repo.checkAssetState(); err != nil {
		return
	}

	// 锁定云端，防止其他设备并发上传数据
	err = repo.tryLockCloud(repo.DeviceID, context)
	if nil != err {
		return
	}
	defer repo.unlockCloud(context)

	mergeResult = &MergeResult{Time: time.Now()}
	trafficStat = &TrafficStat{m: &sync.Mutex{}}

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

	if cloudLatest.ID == latest.ID || "" == cloudLatest.ID {
		// 数据一致或者云端为空，直接返回
		if repo.assetDownloads != nil && cloudLatest.ID == latest.ID {
			err = repo.UpdateLatestSync(latest)
			if err == nil && !repo.assetDownloads.onDemand {
				err = repo.ensureAllAssets(context)
			}
		}
		return
	}

	// 计算本地缺失的文件
	fetchFileIDs, err := repo.localNotFoundFiles(cloudLatest.Files)
	if nil != err {
		logging.LogErrorf("get local not found files failed: %s", err)
		return
	}

	// 下载缺失文件并入库
	fileDownloadStat, _, err := repo.downloadCloudFilesPut(fetchFileIDs, context)
	if nil != err {
		logging.LogErrorf("download cloud files put failed: %s", err)
		return
	}
	trafficStat.DownloadFileCount += len(fetchFileIDs)
	trafficStat.DownloadBytes += fileDownloadStat.CloudBytes
	trafficStat.APIGet += len(fetchFileIDs) - fileDownloadStat.PeerCount
	trafficStat.PeerDownloadBytes += fileDownloadStat.PeerBytes
	trafficStat.PeerDownloadFileCount += fileDownloadStat.PeerCount
	trafficStat.PeerFallbackCount += fileDownloadStat.PeerFallbackCount

	// 组装还原云端最新文件列表
	cloudLatestFiles, err := repo.getFiles(cloudLatest.Files)
	if nil != err {
		logging.LogErrorf("get cloud latest files failed: %s", err)
		return
	}

	// 从文件列表中得到去重后的分块列表
	cloudChunkIDs := repo.getChunks(cloudLatestFiles)

	// 计算本地缺失的分块
	fetchChunkIDs, err := repo.localNotFoundChunks(repo.getChunks(repo.downloadedCloudFiles(cloudLatestFiles)))
	if nil != err {
		logging.LogErrorf("get local not found chunks failed: %s", err)
		return
	}

	// 下载缺失分块并入库
	downloadStat, downloadErr := repo.downloadCloudChunksPut(fetchChunkIDs, context)
	err = downloadErr
	if nil != err {
		logging.LogErrorf("download chunks put failed: %s", err)
		return
	}
	trafficStat.DownloadBytes += downloadStat.CloudBytes
	trafficStat.DownloadChunkCount += len(fetchChunkIDs)
	trafficStat.APIGet += len(fetchChunkIDs) - downloadStat.PeerCount
	trafficStat.PeerDownloadBytes += downloadStat.PeerBytes
	trafficStat.PeerDownloadChunkCount += downloadStat.PeerCount
	trafficStat.PeerFallbackCount += downloadStat.PeerFallbackCount

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
	localUpserts, localRemoves := repo.diffUpsertRemove(latestFiles, latestSyncFiles, false)
	localChanged := 0 < len(localUpserts) || 0 < len(localRemoves)

	// 按 git pull 的语义合并：以上次同步点为基准，云端有的变更拉下来，本地独有的变更保留，两端都改的才算冲突（冲突以云端为准）。
	// 只有内容相同、仅元数据不同的文件按云端元数据还原，保证两端索引收敛。
	now := mergeResult.Time.Format("2006-01-02-150405")
	versionsList := classifySyncFileVersions(latestSyncFiles, latestFiles, cloudLatestFiles)
	for _, versions := range versionsList {
		base, local, cloudFile := versions.Base, versions.Local, versions.Cloud
		localContentChanged := versions.LocalDelta.contentChanged()
		cloudContentChanged := versions.CloudDelta.contentChanged()
		switch {
		case nil == cloudFile:
			if nil == local || nil == base {
				continue // 两端都没有，或者是本地新建的文件：保留
			}
			// 云端删除了上次同步点存在的文件
			mergeResult.Removes = append(mergeResult.Removes, local)
			if localContentChanged {
				mergeResult.Conflicts = append(mergeResult.Conflicts, local)
				logging.LogInfof("sync download conflict [%s, %s, %s]", local.ID, local.Path, time.UnixMilli(local.Updated).Format("2006-01-02 15:04:05"))
			}
		case nil == local:
			if nil == base || cloudContentChanged {
				mergeResult.Upserts = append(mergeResult.Upserts, cloudFile) // 云端新建，或者云端更新了本地已删除的文件：还原云端版本
			}
			// 本地删除且云端未变：保留本地删除
		case equalFileContent(local, cloudFile):
			if !equalFile(local, cloudFile) {
				mergeResult.Upserts = append(mergeResult.Upserts, cloudFile) // 内容相同仅时间戳不同：按云端元数据还原，不算冲突
			}
		case localContentChanged && !cloudContentChanged:
			// 本地修改了云端没动过的文件：保留本地修改
		case !localContentChanged:
			mergeResult.Upserts = append(mergeResult.Upserts, cloudFile)
		default:
			// 两端都修改了同一个文件
			if merged := repo.mergeStructuredSyncFile(base, local, cloudFile, now, context); nil != merged {
				mergeResult.Upserts = append(mergeResult.Upserts, merged)
				mergeResult.MergedPaths = append(mergeResult.MergedPaths, local.Path)
				logging.LogInfof("sync download structured merge [%s, %s]", merged.ID, merged.Path)
				continue
			}
			mergeResult.Upserts = append(mergeResult.Upserts, cloudFile)
			mergeResult.Conflicts = append(mergeResult.Conflicts, local)
			logging.LogInfof("sync download conflict [%s, %s, %s]", local.ID, local.Path, time.UnixMilli(local.Updated).Format("2006-01-02 15:04:05"))
		}
	}
	var ignoredAssets map[string]bool
	if repo.usesAssetDownloads() {
		matcher, matcherErr := repo.cloudAssetIgnoreMatcher(cloudLatestFiles, context)
		if matcherErr != nil {
			return mergeResult, trafficStat, matcherErr
		}
		if ignoredAssets, err = repo.materializeIgnoredAssets(matcher, context); err != nil {
			return
		}
		for _, file := range latestFiles {
			if matcher.MatchesPath(file.Path) {
				ignoredAssets[file.Path] = true
			}
		}
		var removes []*entity.File
		for _, file := range mergeResult.Removes {
			if !ignoredAssets[file.Path] {
				removes = append(removes, file)
			}
		}
		mergeResult.Removes = removes
	}

	// 冲突文件复制到数据历史文件夹
	if 0 < len(mergeResult.Conflicts) {
		temp := filepath.Join(repo.TempPath, "repo", "sync", "conflicts", now)
		for i, file := range mergeResult.Conflicts {
			var checkoutTmp *entity.File
			checkoutTmp, err = repo.store.GetFile(file.ID)
			if nil != err {
				logging.LogErrorf("get file failed: %s", err)
				return
			}

			err = repo.checkoutFile(checkoutTmp, temp, i+1, len(mergeResult.Conflicts), context)
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
			mergeResult.HistoryPaths = append(mergeResult.HistoryPaths, file.Path)
		}
	}

	// 数据变更后还原文件
	if repo.usesAssetDownloads() {
		err = repo.finishAssetSync(mergeResult, localChanged, false, latest, cloudLatest, cloudChunkIDs, trafficStat, context, ignoredAssets)
		if err == nil {
			err = repo.keepCloudSyncPoint(mergeResult, cloudLatest)
		}
		if err == nil {
			go repo.cloud.AddTraffic(&cloud.Traffic{DownloadBytes: trafficStat.DownloadBytes, APIGet: trafficStat.APIGet})
			gulu.File.RemoveEmptyDirs(repo.DataPath, removeEmptyDirExcludes...)
		}
		return
	}
	err = repo.restoreFiles(mergeResult, context)
	if nil != err {
		logging.LogErrorf("restore files failed: %s", err)
		return
	}

	// 处理合并
	err = repo.mergeSync(mergeResult, localChanged, false, latest, cloudLatest, cloudChunkIDs, trafficStat, context)
	if nil != err {
		logging.LogErrorf("merge sync failed: %s", err)
		return
	}
	if err = repo.keepCloudSyncPoint(mergeResult, cloudLatest); nil != err {
		return
	}

	// 统计流量
	go repo.cloud.AddTraffic(&cloud.Traffic{
		DownloadBytes: trafficStat.DownloadBytes,
		APIGet:        trafficStat.APIGet,
	})

	// 移除空目录
	gulu.File.RemoveEmptyDirs(repo.DataPath, removeEmptyDirExcludes...)
	return
}

// keepCloudSyncPoint 在仅下载同步后把同步点保持在云端索引上：
// 保留下来的本地变更和结构化合并结果都还没有上传，下次同步必须把它们作为本地变更上传，而不是当作已同步内容被云端版本覆盖。
func (repo *Repo) keepCloudSyncPoint(mergeResult *MergeResult, cloudLatest *entity.Index) (err error) {
	if nil == cloudLatest || "" == cloudLatest.ID {
		return
	}
	if err = repo.store.PutIndex(cloudLatest); nil != err {
		logging.LogErrorf("put cloud latest index failed: %s", err)
		return
	}
	if err = repo.UpdateLatestSync(cloudLatest); nil != err {
		logging.LogErrorf("update latest sync failed: %s", err)
	}
	return
}

func (repo *Repo) SyncUpload(context map[string]interface{}) (trafficStat *TrafficStat, err error) {
	lock.Lock()
	defer lock.Unlock()
	if err = repo.checkAssetState(); err != nil {
		return
	}

	// 锁定云端，防止其他设备并发上传数据
	err = repo.tryLockCloud(repo.DeviceID, context)
	if nil != err {
		return
	}
	defer repo.unlockCloud(context)

	trafficStat = &TrafficStat{m: &sync.Mutex{}}

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
	trafficStat.APIPut++

	if cloudLatest.ID == latest.ID {
		// 数据一致，直接返回
		return
	}

	availableSize := repo.cloud.GetAvailableSize()
	if availableSize <= cloudLatest.Size || availableSize <= latest.Size {
		err = ErrCloudStorageSizeExceeded
		return
	}

	// 计算云端缺失的文件
	var uploadFiles []*entity.File
	for _, localFileID := range latest.Files {
		if !gulu.Str.Contains(localFileID, cloudLatest.Files) {
			var uploadFile *entity.File
			uploadFile, err = repo.store.GetFile(localFileID)
			if nil != err {
				logging.LogErrorf("get file failed: %s", err)
				return
			}
			uploadFiles = append(uploadFiles, uploadFile)
		}
	}

	// 从文件列表中得到去重后的分块列表
	uploadChunkIDs := repo.getChunks(uploadFiles)
	if repo.assetDownloads != nil {
		for _, file := range uploadFiles {
			if err = repo.ensureFileChunks(file, context); err != nil {
				return
			}
		}
	}

	// 这里暂时不计算云端缺失的分块了，因为目前计数云端缺失分块的代价太大
	//uploadChunkIDs, err = repo.cloud.GetChunks(uploadChunkIDs)
	//if nil != err {
	//	logging.LogErrorf("get cloud repo upload chunks failed: %s", err)
	//	return
	//}

	// 上传分块
	length, err = repo.uploadChunks(uploadChunkIDs, context)
	if nil != err {
		logging.LogErrorf("upload chunks failed: %s", err)
		return
	}
	trafficStat.UploadChunkCount += len(uploadChunkIDs)
	trafficStat.UploadBytes += length
	trafficStat.APIPut += trafficStat.UploadChunkCount

	// 上传文件
	length, err = repo.uploadFiles(uploadFiles, context)
	if nil != err {
		logging.LogErrorf("upload files failed: %s", err)
		return
	}
	trafficStat.UploadChunkCount += len(uploadFiles)
	trafficStat.UploadBytes += length
	trafficStat.APIPut += trafficStat.UploadChunkCount

	// 更新云端索引信息
	err = repo.updateCloudIndexes(latest, trafficStat, context)
	if nil != err {
		logging.LogErrorf("update cloud indexes failed: %s", err)
		return
	}

	// 更新本地同步点
	err = repo.UpdateLatestSync(latest)
	if nil != err {
		logging.LogErrorf("update latest sync failed: %s", err)
		return
	}

	// 统计流量
	go repo.cloud.AddTraffic(&cloud.Traffic{
		UploadBytes: trafficStat.UploadBytes,
		APIPut:      trafficStat.APIPut,
	})
	return
}
