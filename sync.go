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
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/88250/gulu"
	"github.com/88250/lute"
	"github.com/88250/lute/ast"
	"github.com/88250/lute/parse"
	"github.com/panjf2000/ants/v2"
	ignore "github.com/sabhiram/go-gitignore"
	"github.com/siyuan-note/dataparser"
	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
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

	UpsertPetals []string // storage/petal/petals.json 中变更的插件，在思源中计算并填充
	RemovePetals []string // storage/petal/petals.json 中删除的插件，在思源中计算并填充
}

func (mr *MergeResult) DataChanged() bool {
	return len(mr.Upserts) > 0 || len(mr.Removes) > 0 || len(mr.Conflicts) > 0
}

type DownloadTrafficStat struct {
	DownloadFileCount      int
	DownloadChunkCount     int
	DownloadBytes          int64
	PeerDownloadChunkCount int
	PeerDownloadBytes      int64
	PeerFallbackCount      int
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

	m *sync.Mutex
}

func (repo *Repo) GetSyncCloudFiles(cloudLatest *entity.Index, context map[string]interface{}) (fetchedFiles []*entity.File, err error) {
	lock.Lock()
	defer lock.Unlock()

	fetchedFiles, err = repo.getSyncCloudFiles(cloudLatest, context)
	return
}

func (repo *Repo) GetCloudLatest(context map[string]interface{}) (cloudLatest *entity.Index, err error) {
	_, cloudLatest, err = repo.downloadCloudLatest(context)
	return
}

func (repo *Repo) Sync(context map[string]interface{}) (mergeResult *MergeResult, trafficStat *TrafficStat, err error) {
	lock.Lock()
	defer lock.Unlock()

	skipCloudPreflight, _ := context["skipCloudPreflight"].(bool)
	if !skipCloudPreflight {
		mergeResult = &MergeResult{Time: time.Now()}
		trafficStat = &TrafficStat{m: &sync.Mutex{}}
		latest, latestErr := repo.Latest()
		if nil != latestErr {
			logging.LogErrorf("get latest failed: %s", latestErr)
			err = latestErr
			return
		}
		length, cloudLatest, latestErr := repo.downloadCloudLatest(context)
		if nil != latestErr {
			if !errors.Is(latestErr, cloud.ErrCloudObjectNotFound) {
				logging.LogErrorf("download cloud latest failed: %s", latestErr)
				err = latestErr
				return
			}
		}
		trafficStat.DownloadFileCount++
		trafficStat.DownloadBytes += length
		trafficStat.APIGet++
		if cloudLatest.ID == latest.ID {
			// 数据一致时不获取云端锁，减少无变更同步的远程请求。
			return
		}
	}

	// 锁定云端，防止其他设备并发上传数据
	err = repo.tryLockCloud(repo.DeviceID, context)
	if nil != err {
		return
	}
	defer repo.unlockCloud(context)

	mergeResult, trafficStat, err = repo.sync(context)
	if e, ok := err.(*os.PathError); ok && isNoSuchFileOrDirErr(err) {
		p := e.Path
		if !strings.Contains(p, "objects") {
			return
		}

		// 索引时正常，但是上传时可能因为外部变更导致对象（文件或者分块）不存在，此时需要告知用户数据仓库已经损坏，需要重置数据仓库
		logging.LogErrorf("sync failed: %s", err)
		err = ErrRepoFatal
	}
	return
}

func (repo *Repo) sync(context map[string]interface{}) (mergeResult *MergeResult, trafficStat *TrafficStat, err error) {
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

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(1)
	var errs []error
	errsLock := sync.Mutex{}
	go func() { // 从云端下载缺失分块并入库
		defer waitGroup.Done()

		fetchChunkIDs, downloadErr := repo.localNotFoundChunks(cloudChunkIDs)
		if nil != downloadErr {
			logging.LogErrorf("get local not found chunks failed: %s", downloadErr)
			errsLock.Lock()
			errs = append(errs, downloadErr)
			errsLock.Unlock()
			return
		}

		downloadStat, downloadErr := repo.downloadCloudChunksPut(fetchChunkIDs, context)
		if nil != downloadErr {
			logging.LogErrorf("download cloud chunks put failed: %s", downloadErr)
			errsLock.Lock()
			errs = append(errs, downloadErr)
			errsLock.Unlock()
			return
		}
		trafficStat.m.Lock()
		trafficStat.DownloadBytes += downloadStat.CloudBytes
		trafficStat.DownloadChunkCount += len(fetchChunkIDs)
		trafficStat.APIGet += len(fetchChunkIDs) - downloadStat.PeerCount
		trafficStat.PeerDownloadBytes += downloadStat.PeerBytes
		trafficStat.PeerDownloadChunkCount += downloadStat.PeerCount
		trafficStat.PeerFallbackCount += downloadStat.PeerFallbackCount
		trafficStat.m.Unlock()
	}()

	waitGroup.Add(1)
	go func() { // 上传差异数据
		defer waitGroup.Done()

		uploadErr := repo.uploadCloud(context, latest, cloudLatest, cloudChunkIDs, trafficStat)
		if nil != uploadErr {
			logging.LogErrorf("upload cloud failed: %s", uploadErr)
			errsLock.Lock()
			errs = append(errs, uploadErr)
			errsLock.Unlock()
			return
		}
	}()
	waitGroup.Wait()
	errsLock.Lock()
	if 0 < len(errs) {
		err = errs[0]
		errsLock.Unlock()
		return
	}
	errsLock.Unlock()

	// 计算本地相比上一个同步点的 upsert 和 remove 差异
	latestFiles, err := repo.getFiles(latest.Files)
	if nil != err {
		logging.LogErrorf("get latest files failed: %s", err)
		return
	}
	logging.LogInfof("got local latest [%s] files [%d]", latest.ID, len(latestFiles))
	latestSync := repo.latestSync()
	latestSyncFiles, err := repo.getFiles(latestSync.Files)
	if nil != err {
		logging.LogErrorf("get latest sync files failed: %s", err)
		return
	}
	localUpserts, localRemoves := repo.diffUpsertRemove(latestFiles, latestSyncFiles, false)

	latestFileMap := map[string]*entity.File{}
	for _, file := range latestFiles {
		latestFileMap[file.Path] = file
	}

	// 计算云端最新相比本地最新的 upsert 和 remove 差异
	var cloudUpserts, cloudRemoves []*entity.File
	if "" != cloudLatest.ID {
		cloudUpserts, cloudRemoves = repo.diffUpsertRemove(cloudLatestFiles, latestFiles, true)
	}

	// 增加一些诊断日志 https://ld246.com/article/1698370932077
	for _, c := range cloudUpserts {
		logging.LogInfof("cloud upsert [%s, %s, %s]", c.ID, c.Path, time.UnixMilli(c.Updated).Format("2006-01-02 15:04:05"))
	}
	for _, r := range cloudRemoves {
		logging.LogInfof("cloud remove [%s, %s, %s]", r.ID, r.Path, time.UnixMilli(r.Updated).Format("2006-01-02 15:04:05"))
	}
	for _, c := range localUpserts {
		logging.LogInfof("local upsert [%s, %s, %s]", c.ID, c.Path, time.UnixMilli(c.Updated).Format("2006-01-02 15:04:05"))
	}
	for _, r := range localRemoves {
		logging.LogInfof("local remove [%s, %s, %s]", r.ID, r.Path, time.UnixMilli(r.Updated).Format("2006-01-02 15:04:05"))
	}

	// 避免旧的本地数据覆盖云端数据 https://github.com/siyuan-note/siyuan/issues/7403
	localUpserts = repo.filterLocalUpserts(localUpserts, cloudUpserts)
	localChanged := 0 < len(localUpserts) || 0 < len(localRemoves)
	localUpsertsByID := map[string]*entity.File{}
	localUpsertsByPath := map[string]*entity.File{}
	for _, localUpsert := range localUpserts {
		localUpsertsByID[localUpsert.ID] = localUpsert
		localUpsertsByPath[localUpsert.Path] = localUpsert
	}
	localRemovesByID := map[string]*entity.File{}
	localRemovesByPath := map[string]*entity.File{}
	for _, localRemove := range localRemoves {
		localRemovesByID[localRemove.ID] = localRemove
		localRemovesByPath[localRemove.Path] = localRemove
	}
	latestSyncFilesByID := map[string]*entity.File{}
	latestSyncFilesByPath := map[string]*entity.File{}
	for _, latestSyncFile := range latestSyncFiles {
		latestSyncFilesByID[latestSyncFile.ID] = latestSyncFile
		latestSyncFilesByPath[latestSyncFile.Path] = latestSyncFile
	}

	// 记录本地 syncignore 变更
	var localUpsertIgnore *entity.File
	for _, upsert := range localUpserts {
		if "/.siyuan/syncignore" == upsert.Path {
			localUpsertIgnore = upsert
			break
		}
	}

	fetchedFileIDs := map[string]bool{}
	for _, fetchedFile := range fetchedFiles {
		fetchedFileIDs[fetchedFile.ID] = true
	}

	nowStr := mergeResult.Time.Format("2006-01-02-150405")

	// 计算冲突的 upsert 和无冲突能够合并的 upsert
	// 冲突的文件尽量以本地 upsert 和 remove 为准
	var tmpMergeConflicts []*entity.File
	var cloudUpsertIgnore *entity.File
	for _, cloudUpsert := range cloudUpserts {
		if "/.siyuan/syncignore" == cloudUpsert.Path {
			cloudUpsertIgnore = cloudUpsert
		}

		localUpsert := localUpsertsByPath[cloudUpsert.Path]
		if nil == localUpsert {
			localUpsert = localUpsertsByID[cloudUpsert.ID]
		}
		if nil != localUpsert { // 相同的文件本地发生了变更
			// 无论是否发生实际下载文件，都需要生成本地历史，以确保任何情况下都能够通过数据历史恢复文件
			tmpMergeConflicts = append(tmpMergeConflicts, cloudUpsert)

			if fetchedFileIDs[cloudUpsert.ID] {
				// 发生实际下载文件的情况，尝试解决冲突

				latestSyncFile := latestSyncFilesByPath[localUpsert.Path]
				if nil == latestSyncFile {
					latestSyncFile = latestSyncFilesByID[localUpsert.ID]
				}
				if repo.ignoreLocalUpsert(localUpsert, latestSyncFile, nowStr, context) {
					// 如果能忽略本地变更的话则不算做冲突，进行正常合并
					mergeResult.Upserts = append(mergeResult.Upserts, cloudUpsert)
					logging.LogInfof("sync merge upsert [%s, %s, %s]", cloudUpsert.ID, cloudUpsert.Path, time.UnixMilli(cloudUpsert.Updated).Format("2006-01-02 15:04:05"))
					continue
				}

				// 云端有更新的 upsert 从而导致了冲突，在外部单独处理生成副本
				mergeResult.Conflicts = append(mergeResult.Conflicts, cloudUpsert)
				logging.LogInfof("sync merge conflict [%s, %s, %s]", cloudUpsert.ID, cloudUpsert.Path, time.UnixMilli(cloudUpsert.Updated).Format("2006-01-02 15:04:05"))
			}
			continue
		}

		localRemove := localRemovesByPath[cloudUpsert.Path]
		if nil == localRemove {
			localRemove = localRemovesByID[cloudUpsert.ID]
		}
		if nil == localRemove {
			if strings.HasSuffix(cloudUpsert.Path, ".tmp") {
				// 数据仓库不迁出 `.tmp` 临时文件 https://github.com/siyuan-note/siyuan/issues/7087
				logging.LogWarnf("ignored tmp file [%s]", cloudUpsert.Path)
				continue
			}

			// 如果云端 upsert 早于本地已经存在的文件 7 分钟，则以本地文件为准
			cloudUpsertTooOld := false
			if localFile := latestFileMap[cloudUpsert.Path]; nil != localFile && localFile.Updated > cloudUpsert.Updated+7*60*1000 {
				logging.LogWarnf("ignored cloud upsert [%s, %s, %s] because local file is newer", cloudUpsert.ID, cloudUpsert.Path, time.UnixMilli(cloudUpsert.Updated).Format("2006-01-02 15:04:05"))
				cloudUpsertTooOld = true
			}
			if !cloudUpsertTooOld {
				mergeResult.Upserts = append(mergeResult.Upserts, cloudUpsert)
				logging.LogInfof("sync merge upsert [%s, %s, %s]", cloudUpsert.ID, cloudUpsert.Path, time.UnixMilli(cloudUpsert.Updated).Format("2006-01-02 15:04:05"))
			}
		}
	}

	// 计算能够无冲突合并的 remove，冲突的文件以本地 upsert 为准
	for _, cloudRemove := range cloudRemoves {
		localUpsert := localUpsertsByPath[cloudRemove.Path]
		if nil == localUpsert {
			localUpsert = localUpsertsByID[cloudRemove.ID]
		}
		if nil == localUpsert {
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
		temp := filepath.Join(repo.TempPath, "repo", "sync", "conflicts", nowStr)
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
			err = repo.genSyncHistory(nowStr, file.Path, absPath)
			if nil != err {
				logging.LogErrorf("generate sync history failed: %s", err)
				err = ErrCloudGenerateConflictHistory
				return
			}
		}
	}

	// 数据变更后还原文件
	err = repo.restoreFiles(mergeResult, context)
	if nil != err {
		logging.LogErrorf("restore files failed: %s", err)
		return
	}

	// 处理合并
	err = repo.mergeSync(mergeResult, localChanged, true, latest, cloudLatest, cloudChunkIDs, trafficStat, context)
	if nil != err {
		logging.LogErrorf("merge sync failed: %s", err)
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
	gulu.File.RemoveEmptyDirs(repo.DataPath, removeEmptyDirExcludes...)
	return
}

func (repo *Repo) ignoreLocalUpsert(localUpsert, latestSyncFile *entity.File, now string, context map[string]interface{}) bool {
	if !strings.HasSuffix(localUpsert.Path, ".sy") {
		return false // 非 .sy 文件目前不做内容对比，直接认为本地 upsert 是最新的
	}

	if nil == latestSyncFile {
		return false // 本地 upsert 是新增的文件
	}

	// 如果是变更 .sy 文件则需要解析并进行内容对比

	luteEngine := lute.New()
	temp := filepath.Join(repo.TempPath, "repo", "sync", "resolves", now)
	localTree, err := repo.checkoutTree(localUpsert, temp, luteEngine, context)
	if nil != err {
		return false
	}
	localLastSyncTree, err := repo.checkoutTree(latestSyncFile, temp, luteEngine, context)
	if nil != err {
		return false
	}

	localNodes, localLastSyncNodes := map[string]*ast.Node{}, map[string]*ast.Node{}
	ast.Walk(localTree.Root, func(node *ast.Node, entering bool) ast.WalkStatus {
		if !entering || !node.IsBlock() || ast.NodeDocument == node.Type {
			return ast.WalkContinue
		}

		localNodes[node.ID] = node
		return ast.WalkContinue
	})
	ast.Walk(localLastSyncTree.Root, func(node *ast.Node, entering bool) ast.WalkStatus {
		if !entering || !node.IsBlock() || ast.NodeDocument == node.Type {
			return ast.WalkContinue
		}

		localLastSyncNodes[node.ID] = node
		return ast.WalkContinue
	})

	if len(localNodes) != len(localLastSyncNodes) {
		return false // 本地变更导致块数量不相同
	}

	for id, localNode := range localNodes {
		if lastSyncNode, ok := localLastSyncNodes[id]; !ok || localNode.ID != lastSyncNode.ID || localNode.Type != lastSyncNode.Type {
			return false // 本地变更导致块不相同
		}

		localLastSyncNode := localLastSyncNodes[id]
		if !onlyChangeFoldIAL(localNode, localLastSyncNode) {
			return false // 本地变更导致块不相同
		}
	}
	return true // 本地仅变更了折叠属性，并且云端也有更新的 upsert，所以忽略本地的折叠变更
}

func onlyChangeFoldIAL(n1, n2 *ast.Node) bool {
	if n1.Content() != n2.Content() {
		return false
	}

	n1Attrs := parse.IAL2Map(n1.KramdownIAL)
	n2Attrs := parse.IAL2Map(n2.KramdownIAL)

	// 移除折叠属性
	delete(n1Attrs, "fold")
	delete(n1Attrs, "heading-fold")
	delete(n2Attrs, "fold")
	delete(n2Attrs, "heading-fold")

	// 移除更新时间
	delete(n1Attrs, "updated")
	delete(n2Attrs, "updated")

	if len(n1Attrs) != len(n2Attrs) {
		return false
	}

	for k, v1 := range n1Attrs {
		if v2, ok := n2Attrs[k]; !ok || v1 != v2 {
			return false
		}
	}
	return true
}

func (repo *Repo) checkoutTree(file *entity.File, checkoutDir string, luteEngine *lute.Lute, context map[string]interface{}) (ret *parse.Tree, err error) {
	checkoutTmp, err := repo.store.GetFile(file.ID)
	if nil != err {
		logging.LogErrorf("get file failed: %s", err)
		return
	}
	if err = repo.checkoutFile(checkoutTmp, checkoutDir, 1, 1, context); nil != err {
		logging.LogErrorf("checkout file failed: %s", err)
		return
	}
	absPath := filepath.Join(checkoutDir, checkoutTmp.Path)
	data, err := os.ReadFile(absPath)
	if nil != err {
		logging.LogErrorf("read file failed: %s", err)
		return
	}
	ret, err = dataparser.ParseJSONWithoutFix(data, luteEngine.ParseOptions)
	if nil != err {
		logging.LogErrorf("parse tree failed: %s", err)
		return
	}
	return
}

func (repo *Repo) restoreFiles(mergeResult *MergeResult, context map[string]interface{}) (err error) {
	err = repo.checkoutFiles(mergeResult.Upserts, context)
	if nil != err {
		logging.LogErrorf("checkout files failed: %s", err)
		return
	}
	err = repo.removeFiles(mergeResult.Removes, context)
	if nil != err {
		logging.LogErrorf("remove files failed: %s", err)
		return
	}
	return
}

func (repo *Repo) mergeSync(mergeResult *MergeResult, localChanged, needSyncCloud bool, latest, cloudLatest *entity.Index, cloudChunkIDs []string, trafficStat *TrafficStat, context map[string]interface{}) (err error) {
	if mergeResult.DataChanged() {
		if localChanged { // 如果云端和本地都改变了，则需要创建合并索引并再次同步
			logging.LogInfof("creating merge index [%s]", latest.ID)
			mergeStart := time.Now()
			mergedLatest, mergeIndexErr := repo.index("[Sync] Cloud sync merge", false, context)
			if nil != mergeIndexErr {
				logging.LogErrorf("merge index failed: %s", mergeIndexErr)
				err = mergeIndexErr
				return
			}

			diff, mergeIndexErr := repo.diffIndex(mergedLatest, latest)
			if nil != mergeIndexErr {
				logging.LogErrorf("diff index failed: %s", mergeIndexErr)
				err = mergeIndexErr
				return
			}
			for _, add := range diff.AddsLeft {
				logging.LogInfof("merge index add [%s, %s, %s]", add.ID, add.Path, time.UnixMilli(add.Updated).Format("2006-01-02 15:04:05"))
			}
			for _, update := range diff.UpdatesLeft {
				logging.LogInfof("merge index update [%s, %s, %s]", update.ID, update.Path, time.UnixMilli(update.Updated).Format("2006-01-02 15:04:05"))
			}

			latest = mergedLatest
			mergeElapsed := time.Since(mergeStart)
			mergeMemo := fmt.Sprintf("[Sync] Cloud sync merge, completed in %.2fs", mergeElapsed.Seconds())
			latest.Memo = mergeMemo
			err = repo.store.PutIndex(latest)
			if nil != err {
				logging.LogErrorf("put merge index failed: %s", err)
				return
			}
			logging.LogInfof("created merge index [%s]", latest.ID)

			if needSyncCloud {
				err = repo.uploadCloud(context, latest, cloudLatest, cloudChunkIDs, trafficStat)
				if nil != err {
					logging.LogErrorf("upload cloud failed: %s", err)
					return
				}
			}
		} else { // 只有云端改变了，本地没有改变，则直接使用云端索引作为本地最新索引
			latest = cloudLatest
		}
	}

	if (localChanged && needSyncCloud) || "" == cloudLatest.ID {
		err = repo.updateCloudIndexes(latest, trafficStat, context)
		if nil != err {
			logging.LogErrorf("update cloud indexes failed: %s", err)
			return
		}
	}

	// 更新本地最新索引
	if err = repo.UpdateLatest(latest); nil != err {
		logging.LogErrorf("update latest failed: %s", err)
		return
	}
	if err = repo.store.PutIndex(latest); nil != err {
		logging.LogErrorf("put index failed: %s", err)
		return
	}

	// 更新本地同步点
	err = repo.UpdateLatestSync(latest)
	if nil != err {
		logging.LogErrorf("update latest sync failed: %s", err)
		return
	}
	return
}

func (repo *Repo) updateCloudIndexes(latest *entity.Index, trafficStat *TrafficStat, context map[string]interface{}) (err error) {
	// 生成校验索引
	files, getErr := repo.getFiles(latest.Files)
	if nil != getErr {
		logging.LogErrorf("get files failed: %s", getErr)
		err = getErr
		return
	}

	checkIndex := &entity.CheckIndex{ID: util.RandHash(), IndexID: latest.ID}
	for _, file := range files {
		checkIndex.Files = append(checkIndex.Files, &entity.CheckIndexFile{ID: file.ID, Chunks: file.Chunks})
	}

	// 更新本地 latest 的关联的 checkIndexID，后续会将本地 latest 上传到云端
	latest.CheckIndexID = checkIndex.ID
	if err = repo.store.PutIndex(latest); nil != err {
		logging.LogErrorf("put index failed: %s", err)
		return
	}

	// 以下步骤是更新云端相关索引数据

	var errs []error
	errLock := sync.Mutex{}
	waitGroup := &sync.WaitGroup{}

	// 更新云端 latest
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		// 上传索引和更新 refs/latest 两个操作需要保证顺序，否则可能会导致云端索引 和 refs/latest 不一致 https://github.com/siyuan-note/siyuan/issues/10111

		// 上传索引
		length, uploadErr := repo.uploadIndex(latest, context)
		if nil != uploadErr {
			logging.LogErrorf("upload latest index failed: %s", uploadErr)
			errLock.Lock()
			errs = append(errs, uploadErr)
			errLock.Unlock()
			return
		}
		trafficStat.m.Lock()
		trafficStat.UploadFileCount++
		trafficStat.UploadBytes += length
		trafficStat.APIPut++
		trafficStat.m.Unlock()

		// 更新 refs/latest
		length, uploadErr = repo.updateCloudRef("refs/latest", context)
		if nil != uploadErr {
			logging.LogErrorf("update cloud [refs/latest] failed: %s", uploadErr)
			errLock.Lock()
			errs = append(errs, uploadErr)
			errLock.Unlock()
			return
		}
		trafficStat.m.Lock()
		trafficStat.UploadFileCount++
		trafficStat.UploadBytes += length
		trafficStat.APIPut++
		trafficStat.m.Unlock()
	}()

	isS3OrSiYuan := repo.isCloudS3() || repo.isCloudSiYuan()
	if isS3OrSiYuan {
		// 上传最新索引列表 https://github.com/siyuan-note/siyuan/issues/12991
		// 上传 refs/latest 后可能存在缓存导致后续下载 refs/latest 时返回的是旧数据，所以这里还需要再上传 refs/latest-seqNum-id，
		// 后续下载 latest 时使用 list 接口返回前缀为 refs/latest- 的对象，然后取最新的一个和下载到的 latest 对比，
		// 如果不一致则重现下载 refs/latest 进行确认，具体细节参考 downloadCloudLatest()
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()

			_, maxSeqNum, seqNumLatests := repo.getSeqNumLatest()
			seqNum := maxSeqNum + 1
			_, uploadErr := repo.cloud.UploadBytes("refs/latest-"+strconv.Itoa(seqNum)+"-"+latest.ID, []byte(latest.ID), true)
			if nil != uploadErr {
				logging.LogErrorf("update cloud [refs/latest-%d] failed: %s", seqNum, uploadErr)
				errLock.Lock()
				errs = append(errs, uploadErr)
				errLock.Unlock()
				return
			}

			// 删除旧的 refs/latest-*
			go func() {
				for _, seqNumLatest := range seqNumLatests {
					deleteErr := repo.cloud.RemoveObject(seqNumLatest)
					if nil != deleteErr {
						logging.LogWarnf("delete cloud [%s] failed: %s", seqNumLatest, deleteErr)
						continue
					}
				}
			}()
		}()
	}

	// 更新云端索引列表
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		downloadBytes, uploadBytes, uploadErr := repo.updateCloudIndexesV2(latest, context)
		if nil != uploadErr {
			logging.LogErrorf("update cloud indexes failed: %s", uploadErr)
			errLock.Lock()
			errs = append(errs, uploadErr)
			errLock.Unlock()
			return
		}

		trafficStat.m.Lock()
		trafficStat.DownloadFileCount++
		trafficStat.DownloadBytes += downloadBytes
		trafficStat.UploadFileCount++
		trafficStat.UploadBytes += uploadBytes
		trafficStat.APIGet++
		trafficStat.APIPut++
		trafficStat.m.Unlock()
	}()

	// 上传校验索引
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		uploadErr := repo.updateCloudCheckIndex(checkIndex, context)
		if nil != uploadErr {
			logging.LogErrorf("update cloud check index failed: %s", uploadErr)
			errLock.Lock()
			errs = append(errs, uploadErr)
			errLock.Unlock()
			return
		}
	}()

	// 尝试上传修复云端缺失的数据对象
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		repo.uploadCloudMissingObjects(trafficStat, context)
	}()

	waitGroup.Wait()

	if 0 < len(errs) {
		err = errs[0]
	}
	return
}

// filterLocalUpserts 避免旧的本地数据覆盖云端数据 https://github.com/siyuan-note/siyuan/issues/7403
func (repo *Repo) filterLocalUpserts(localUpserts, cloudUpserts []*entity.File) (ret []*entity.File) {
	cloudUpsertsMap := map[string]*entity.File{}
	for _, cloudUpsert := range cloudUpserts {
		cloudUpsertsMap[cloudUpsert.Path] = cloudUpsert
	}

	toRemoveLocalUpsertPaths := map[string]bool{}
	for _, localUpsert := range localUpserts {
		if cloudUpsert := cloudUpsertsMap[localUpsert.Path]; nil != cloudUpsert {
			if localUpsert.Updated < cloudUpsert.Updated-1000*60*7 { // 本地早于云端 7 分钟
				toRemoveLocalUpsertPaths[localUpsert.Path] = true // 使用云端数据覆盖本地数据
				logging.LogWarnf("ignored local upsert [%s, %s, %s] because it is older than cloud upsert [%s, %s, %s]",
					localUpsert.ID, localUpsert.Path, time.UnixMilli(localUpsert.Updated).Format("2006-01-02 15:04:05"),
					cloudUpsert.ID, cloudUpsert.Path, time.UnixMilli(cloudUpsert.Updated).Format("2006-01-02 15:04:05"))
			}
		}
	}

	for _, localUpsert := range localUpserts {
		if !toRemoveLocalUpsertPaths[localUpsert.Path] {
			ret = append(ret, localUpsert)
		}
	}

	if len(localUpserts) != len(ret) {
		buf := bytes.Buffer{}
		buf.WriteString("filtered local upserts from:\n")
		for _, localUpsert := range localUpserts {
			buf.WriteString(fmt.Sprintf("  [%s, %s, %s]\n", localUpsert.ID, localUpsert.Path, time.UnixMilli(localUpsert.Updated).Format("2006-01-02 15:04:05")))
		}
		buf.WriteString("to:\n")
		for _, localUpsert := range ret {
			buf.WriteString(fmt.Sprintf("  [%s, %s, %s]\n", localUpsert.ID, localUpsert.Path, time.UnixMilli(localUpsert.Updated).Format("2006-01-02 15:04:05")))
		}
		if 1 > len(ret) {
			buf.WriteString("  []")
		}
		logging.LogWarn(buf.String())
	}
	return
}

func (repo *Repo) getSyncCloudFiles(cloudLatest *entity.Index, context map[string]interface{}) (fetchedFiles []*entity.File, err error) {
	latest, err := repo.Latest()
	if nil != err {
		logging.LogErrorf("get latest failed: %s", err)
		return
	}

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
	trafficStat := &TrafficStat{m: &sync.Mutex{}}
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

func (repo *Repo) downloadCloudChunksPut(chunkIDs []string, context map[string]interface{}) (stat *chunkDownloadStat, err error) {
	stat = &chunkDownloadStat{}
	if 1 > len(chunkIDs) {
		return
	}

	peerChunks := map[string]bool{}
	if nil != repo.chunkSource {
		if found, hasErr := repo.chunkSource.HasChunks(chunkIDs); nil == hasErr {
			peerChunks = found
		} else {
			logging.LogWarnf("query chunk source [%s] failed: %s", repo.chunkSource.Name(), hasErr)
		}
	}

	waitGroup := &sync.WaitGroup{}
	var downloadErr error
	downloadErrLock := sync.Mutex{}
	cloudConcurrentReqs := repo.cloud.GetConcurrentReqs()
	if cloudConcurrentReqs < 1 {
		cloudConcurrentReqs = 1
	}
	peerConcurrentReqs := 0
	if nil != repo.chunkSource {
		peerConcurrentReqs = repo.chunkSource.GetConcurrentReqs()
		if peerConcurrentReqs < 1 {
			peerConcurrentReqs = 1
		}
	}
	poolSize := cloudConcurrentReqs + peerConcurrentReqs
	if poolSize > len(chunkIDs) {
		poolSize = len(chunkIDs)
	}
	cloudSemaphore := make(chan struct{}, cloudConcurrentReqs)
	peerSemaphore := make(chan struct{}, peerConcurrentReqs)
	count := atomic.Int32{}
	cloudBytes := atomic.Int64{}
	peerBytes := atomic.Int64{}
	peerCount := atomic.Int32{}
	peerFallbackCount := atomic.Int32{}
	total := len(chunkIDs)
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		downloadErrLock.Lock()
		if nil != downloadErr {
			downloadErrLock.Unlock()
			return // 快速失败
		}
		downloadErrLock.Unlock()

		chunkID := arg.(string)
		count.Add(1)
		var length int64
		var chunk *entity.Chunk
		var dccErr error
		if peerChunks[chunkID] {
			peerSemaphore <- struct{}{}
			length, chunk, dccErr = repo.downloadSourceChunk(chunkID)
			<-peerSemaphore
			if nil == dccErr {
				peerBytes.Add(length)
				peerCount.Add(1)
			} else {
				peerFallbackCount.Add(1)
				logging.LogWarnf("download chunk [%s] from source [%s] failed, falling back to cloud: %s",
					chunkID, repo.chunkSource.Name(), dccErr)
			}
		}
		if nil == chunk {
			cloudSemaphore <- struct{}{}
			length, chunk, dccErr = repo.downloadCloudChunk(chunkID, int(count.Load()), total, context)
			<-cloudSemaphore
			if nil == dccErr {
				cloudBytes.Add(length)
			}
		}
		if nil != dccErr {
			downloadErrLock.Lock()
			if nil == downloadErr {
				downloadErr = dccErr
			}
			downloadErrLock.Unlock()
			return
		}
		if pcErr := repo.store.PutChunk(chunk); nil != pcErr {
			downloadErrLock.Lock()
			if nil == downloadErr {
				downloadErr = pcErr
			}
			downloadErrLock.Unlock()
			return
		}
	})
	if nil != err {
		return
	}
	defer p.Release()

	eventbus.Publish(eventbus.EvtCloudBeforeDownloadChunks, context, total)
	for _, chunkID := range chunkIDs {
		waitGroup.Add(1)
		if err = p.Invoke(chunkID); nil != err {
			waitGroup.Done()
			logging.LogErrorf("invoke failed: %s", err)
			break
		}
	}
	waitGroup.Wait()
	if nil != err {
		return
	}
	stat.CloudBytes = cloudBytes.Load()
	stat.PeerBytes = peerBytes.Load()
	stat.PeerCount = int(peerCount.Load())
	stat.PeerFallbackCount = int(peerFallbackCount.Load())
	downloadErrLock.Lock()
	defer downloadErrLock.Unlock()
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
	poolSize := repo.cloud.GetConcurrentReqs()
	if poolSize > len(fileIDs) {
		poolSize = len(fileIDs)
	}
	count := atomic.Int32{}
	dBytes := atomic.Int64{}
	total := len(fileIDs)
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != downloadErr {
			return // 快速失败
		}

		fileID := arg.(string)
		count.Add(1)
		length, file, dcfErr := repo.downloadCloudFile(fileID, int(count.Load()), total, context)
		if nil != dcfErr {
			downloadErr = dcfErr
			return
		}
		if pfErr := repo.store.PutFile(file); nil != pfErr {
			downloadErr = pfErr
			return
		}
		dBytes.Add(length)

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
			logging.LogErrorf("invoke failed: %s", err)
			return
		}
		if nil != downloadErr {
			err = downloadErr
			return
		}
	}
	waitGroup.Wait()
	p.Release()
	downloadBytes = dBytes.Load()
	if nil != downloadErr {
		err = downloadErr
		return
	}
	return
}

func (repo *Repo) updateCloudRef(ref string, context map[string]interface{}) (uploadBytes int64, err error) {
	eventbus.Publish(eventbus.EvtCloudBeforeUploadRef, context, ref)
	absFilePath := filepath.Join(repo.cloud.GetConf().RepoPath, ref)
	data, err := os.ReadFile(absFilePath)
	if nil != err {
		logging.LogErrorf("read ref [%s] failed: %s", ref, err)
		return
	}

	length, err := repo.cloud.UploadObject(ref, true)
	if nil != err {
		return
	}
	uploadBytes += length
	logging.LogInfof("uploaded cloud ref [%s, id=%s]", ref, data)
	return
}

var uploadedCloudMissingObjects = false

func (repo *Repo) uploadCloudMissingObjects(trafficStat *TrafficStat, context map[string]interface{}) {
	if uploadedCloudMissingObjects {
		return
	}
	uploadedCloudMissingObjects = true

	if _, ok := repo.cloud.(*cloud.SiYuan); !ok {
		return
	}

	defer eventbus.Publish(eventbus.EvtCloudAfterFixObjects, context)

	checkReportKey := "check/indexes-report"
	data, err := repo.cloud.DownloadObject(checkReportKey)
	if nil != err {
		if errors.Is(err, cloud.ErrCloudObjectNotFound) {
			return
		}

		logging.LogErrorf("download check report failed: %s", err)
		return
	}
	trafficStat.m.Lock()
	trafficStat.DownloadFileCount++
	trafficStat.DownloadBytes += int64(len(data))
	trafficStat.APIGet++
	trafficStat.m.Unlock()

	data, err = repo.store.compressDecoder.DecodeAll(data, nil)
	if nil != err {
		logging.LogErrorf("decompress check report failed: %s", err)
		return
	}

	checkReport := &entity.CheckReport{}
	if err = gulu.JSON.UnmarshalJSON(data, checkReport); nil != err {
		logging.LogErrorf("unmarshal check report failed: %s", err)
		return
	}

	if 1 > len(checkReport.MissingObjects) {
		return
	}

	var missingObjects []string
	stillMissingObjects := map[string]bool{}
	for _, missingObject := range checkReport.MissingObjects {
		logging.LogInfof("cloud missing object [%s]", missingObject)
		stillMissingObjects[missingObject] = true

		absFilePath := filepath.Join(repo.Path, "objects", missingObject)
		info, statErr := os.Stat(absFilePath)
		if nil != statErr {
			// 本地没有该文件，忽略
			logging.LogWarnf("cloud missing object [%s] not found: %s", missingObject, statErr)
			continue
		}

		length := info.Size()
		trafficStat.m.Lock()
		trafficStat.UploadBytes += length
		trafficStat.UploadFileCount++
		trafficStat.m.Unlock()
		missingObjects = append(missingObjects, missingObject)
	}
	missingObjects = gulu.Str.RemoveDuplicatedElem(missingObjects)

	waitGroup := &sync.WaitGroup{}
	var uploadErr error
	poolSize := repo.cloud.GetConcurrentReqs()
	if poolSize > len(missingObjects) {
		poolSize = len(missingObjects)
	}
	count := atomic.Int32{}
	total := len(missingObjects)
	lock := sync.Mutex{}
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != uploadErr {
			return // 快速失败
		}

		objectPath := arg.(string)
		filePath := "objects/" + objectPath
		count.Add(1)
		eventbus.Publish(eventbus.EvtCloudBeforeFixObjects, context, int(count.Load()), total)
		_, uoErr := repo.cloud.UploadObject(filePath, false)
		if nil != uoErr {
			uploadErr = uoErr
			err = uploadErr
			logging.LogErrorf("upload cloud missing object [%s] failed: %s", filePath, uploadErr)
			return
		}

		lock.Lock()
		delete(stillMissingObjects, objectPath)
		lock.Unlock()
		logging.LogInfof("uploaded cloud missing object [%s]", filePath)
	})
	if nil != err {
		logging.LogWarnf("upload cloud missing objects failed: %s", err)
		return
	}

	for _, missingObject := range missingObjects {
		waitGroup.Add(1)
		if err = p.Invoke(missingObject); nil != err {
			logging.LogErrorf("invoke failed: %s", err)
			return
		}
		if nil != uploadErr {
			err = uploadErr
			return
		}
	}
	waitGroup.Wait()
	p.Release()

	if nil != err {
		logging.LogWarnf("upload cloud missing objects failed: %s", err)
		return
	}

	checkReport.FixCount++
	checkReport.MissingObjects = nil
	for missingObject := range stillMissingObjects {
		checkReport.MissingObjects = append(checkReport.MissingObjects, missingObject)
		logging.LogWarnf("cloud still missing object [%s]", missingObject)
	}

	if 0 < len(checkReport.MissingObjects) {
		eventbus.Publish(eventbus.EvtCloudCorrupted)
		logging.LogWarnf("cloud still missing objects [%d]", len(checkReport.MissingObjects))
	} else {
		logging.LogInfof("cloud missing objects fixed")
	}

	data, err = gulu.JSON.MarshalJSON(checkReport)
	if nil != err {
		logging.LogErrorf("marshal check report failed: %s", err)
		return
	}

	data = repo.store.compressEncoder.EncodeAll(data, nil)

	absPath := filepath.Join(repo.Path, checkReportKey)
	if err = gulu.File.WriteFileSafer(absPath, data, 0644); nil != err {
		logging.LogErrorf("write check report failed: %s", err)
		return
	}

	if _, err = repo.cloud.UploadObject(checkReportKey, true); nil != err {
		logging.LogErrorf("upload check report failed: %s", err)
	}
	return
}

func (repo *Repo) updateCloudCheckIndex(checkIndex *entity.CheckIndex, context map[string]interface{}) (err error) {
	if _, ok := repo.cloud.(*cloud.SiYuan); !ok {
		// S3/WebDAV 不上传校验索引 S3/WebDAV data sync no longer uploads check index https://github.com/siyuan-note/siyuan/issues/10180
		return
	}

	eventbus.Publish(eventbus.EvtCloudBeforeUploadCheckIndex, context)

	data, marshalErr := gulu.JSON.MarshalIndentJSON(checkIndex, "", "\t")
	if nil != marshalErr {
		logging.LogErrorf("marshal check index failed: %s", marshalErr)
		err = marshalErr
		return
	}

	data = repo.store.compressEncoder.EncodeAll(data, nil)

	dir := filepath.Join(repo.Path, "check", "indexes")
	if err = os.MkdirAll(dir, 0755); nil != err {
		return
	}

	if err = gulu.File.WriteFileSafer(filepath.Join(dir, checkIndex.ID), data, 0644); nil != err {
		logging.LogErrorf("write check index failed: %s", err)
		return
	}

	if _, err = repo.cloud.UploadObject("check/indexes/"+checkIndex.ID, false); nil != err {
		logging.LogErrorf("upload check index failed: %s", err)
		return
	}
	return
}

func (repo *Repo) updateCloudIndexesV2(latest *entity.Index, context map[string]interface{}) (downloadBytes, uploadBytes int64, err error) {
	eventbus.Publish(eventbus.EvtCloudBeforeUploadIndexes, context)

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
		logging.LogErrorf("decompress cloud indexes-v2.json failed: %s", err)
		return
	}

	indexes := &cloud.Indexes{}
	if 0 < len(data) {
		if err = gulu.JSON.UnmarshalJSON(data, &indexes); nil != err {
			logging.LogWarnf("unmarshal cloud indexes-v2.json failed: %s", err)
			return
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
		logging.LogErrorf("marshal cloud indexes-v2.json failed: %s", err)
		return
	}

	data = repo.store.compressEncoder.EncodeAll(data, nil)

	if err = gulu.File.WriteFileSafer(filepath.Join(repo.Path, "indexes-v2.json"), data, 0644); nil != err {
		logging.LogErrorf("write indexes-v2.json failed: %s", err)
		return
	}

	length, err := repo.cloud.UploadObject("indexes-v2.json", true)
	uploadBytes = length
	return
}

func (repo *Repo) uploadIndex(index *entity.Index, context map[string]interface{}) (uploadBytes int64, err error) {
	eventbus.Publish(eventbus.EvtCloudBeforeUploadIndex, context, index.ID)
	length, err := repo.cloud.UploadObject(path.Join("indexes", index.ID), false)
	if nil != err {
		return
	}
	uploadBytes += length
	logging.LogInfof("uploaded index [%s]", index.String())
	return
}

func (repo *Repo) uploadFiles(upsertFiles []*entity.File, context map[string]interface{}) (uploadBytes int64, err error) {
	if 1 > len(upsertFiles) {
		return
	}

	waitGroup := &sync.WaitGroup{}
	var uploadErr error
	uploadErrLock := sync.Mutex{}
	uploadBytesAtomic := atomic.Int64{}
	poolSize := repo.cloud.GetConcurrentReqs()
	if poolSize > len(upsertFiles) {
		poolSize = len(upsertFiles)
	}
	count, uploadedCount := atomic.Int32{}, atomic.Int32{}
	total := len(upsertFiles)
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		uploadErrLock.Lock()
		if nil != uploadErr {
			uploadErrLock.Unlock()
			return // 快速失败
		}
		uploadErrLock.Unlock()

		upsertFileID := arg.(string)
		filePath := path.Join("objects", upsertFileID[:2], upsertFileID[2:])
		count.Add(1)
		eventbus.Publish(eventbus.EvtCloudBeforeUploadFile, context, int(count.Load()), total)
		length, uoErr := repo.cloud.UploadObject(filePath, false)
		if nil != uoErr {
			uploadErrLock.Lock()
			if nil == uploadErr {
				uploadErr = uoErr
			}
			uploadErrLock.Unlock()
			return
		}
		uploadBytesAtomic.Add(length)
		uploadedCount.Add(1)
		//logging.LogInfof("uploaded file [%s, %d/%d]", filePath, int(uploadedCount.Load()), total)
	})
	if nil != err {
		return
	}

	eventbus.Publish(eventbus.EvtCloudBeforeUploadFiles, context, total)
	for _, upsertFile := range upsertFiles {
		waitGroup.Add(1)
		if err = p.Invoke(upsertFile.ID); nil != err {
			waitGroup.Done()
			logging.LogErrorf("invoke failed: %s", err)
			break
		}
	}
	waitGroup.Wait()
	p.Release()
	uploadBytes = uploadBytesAtomic.Load()
	if nil != err {
		return
	}
	uploadErrLock.Lock()
	err = uploadErr
	uploadErrLock.Unlock()
	return
}

func (repo *Repo) uploadChunks(upsertChunkIDs []string, context map[string]interface{}) (uploadBytes int64, err error) {
	if 1 > len(upsertChunkIDs) {
		return
	}

	waitGroup := &sync.WaitGroup{}
	var uploadErr error
	uploadErrLock := sync.Mutex{}
	uploadBytesAtomic := atomic.Int64{}
	poolSize := repo.cloud.GetConcurrentReqs()
	if poolSize > len(upsertChunkIDs) {
		poolSize = len(upsertChunkIDs)
	}
	count, uploadedCount := atomic.Int32{}, atomic.Int32{}
	total := len(upsertChunkIDs)
	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		uploadErrLock.Lock()
		if nil != uploadErr {
			uploadErrLock.Unlock()
			return // 快速失败
		}
		uploadErrLock.Unlock()

		upsertChunkID := arg.(string)
		filePath := path.Join("objects", upsertChunkID[:2], upsertChunkID[2:])
		count.Add(1)
		eventbus.Publish(eventbus.EvtCloudBeforeUploadChunk, context, int(count.Load()), total)
		length, uoErr := repo.cloud.UploadObject(filePath, false)
		if nil != uoErr {
			uploadErrLock.Lock()
			if nil == uploadErr {
				uploadErr = uoErr
			}
			uploadErrLock.Unlock()
			return
		}
		uploadBytesAtomic.Add(length)
		uploadedCount.Add(1)
		//logging.LogInfof("uploaded chunk [%s, %d/%d]", filePath, int(uploadedCount.Load()), total)
	})
	if nil != err {
		return
	}

	eventbus.Publish(eventbus.EvtCloudBeforeUploadChunks, context, total)
	for _, upsertChunkID := range upsertChunkIDs {
		waitGroup.Add(1)
		if err = p.Invoke(upsertChunkID); nil != err {
			waitGroup.Done()
			logging.LogErrorf("invoke failed: %s", err)
			break
		}
	}
	waitGroup.Wait()
	p.Release()
	uploadBytes = uploadBytesAtomic.Load()
	if nil != err {
		return
	}
	uploadErrLock.Lock()
	err = uploadErr
	uploadErrLock.Unlock()
	return
}

func (repo *Repo) localNotFoundChunks(chunkIDs []string) (ret []string, err error) {
	for _, chunkID := range chunkIDs {
		if _, getChunkErr := repo.store.Stat(chunkID); nil != getChunkErr {
			if isNoSuchFileOrDirErr(getChunkErr) {
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
			if isNoSuchFileOrDirErr(getFileErr) {
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
		//logging.LogInfof("upsert file [%s, %s, %s] chunk [%s]",
		//	file.ID, file.Path, time.UnixMilli(file.Updated).Format("2006-01-02 15:04:05"), strings.Join(file.Chunks, ","))
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

	//for _, c := range ret {
	//	logging.LogInfof("upsert chunk [%s]", c)
	//}
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

func (repo *Repo) UpdateLatestSync(index *entity.Index) (err error) {
	refs := filepath.Join(repo.Path, "refs")
	err = os.MkdirAll(refs, 0755)
	if nil != err {
		return
	}
	err = gulu.File.WriteFileSafer(filepath.Join(refs, "latest-sync"), []byte(index.ID), 0644)
	if nil != err {
		return
	}
	logging.LogInfof("updated latest sync [%s]", index.String())
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
	trafficStat.m.Lock()
	trafficStat.UploadChunkCount += len(upsertChunkIDs)
	trafficStat.UploadBytes += length
	trafficStat.APIPut += len(upsertChunkIDs)
	trafficStat.m.Unlock()

	// 上传文件
	length, err = repo.uploadFiles(upsertFiles, context)
	if nil != err {
		logging.LogErrorf("upload files failed: %s", err)
		return
	}
	trafficStat.m.Lock()
	trafficStat.UploadFileCount += len(upsertFiles)
	trafficStat.UploadBytes += length
	trafficStat.APIPut += len(upsertFiles)
	trafficStat.m.Unlock()
	return
}

func (repo *Repo) latestSync() (ret *entity.Index) {
	ret = &entity.Index{} // 构造一个空的索引表示没有同步点

	latestSync := filepath.Join(repo.Path, "refs", "latest-sync")
	if !filelock.IsExist(latestSync) {
		logging.LogInfof("latest sync index not found, return an empty index")
		return
	}

	data, err := filelock.ReadFile(latestSync)
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
		ret = &entity.Index{}
		return
	}
	logging.LogInfof("got latest sync [%s]", ret.String())
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
	if util.Hash(data) != id {
		err = ErrRepoFatal
		logging.LogErrorf("cloud chunk [%s] hash mismatch", id)
		return
	}
	ret = &entity.Chunk{ID: id, Data: data}
	return
}

func (repo *Repo) downloadSourceChunk(id string) (length int64, ret *entity.Chunk, err error) {
	data, err := repo.chunkSource.DownloadChunk(id)
	if nil != err {
		return
	}
	key := path.Join("objects", id[:2], id[2:])
	data, err = repo.decodeDownloadedData(key, data)
	if nil != err {
		return
	}
	if util.Hash(data) != id {
		err = fmt.Errorf("%w: source chunk [%s] hash mismatch", ErrRepoFatal, id)
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

	if !index.VerifyAESKey(repo.store.AesKey) {
		err = cloud.ErrDecryptFailed
		logging.LogErrorf("cloud index [%s] verify AES key failed", index.String())
		return
	}
	return
}

var downloadCloudLatestLock = sync.Mutex{}

func (repo *Repo) downloadCloudLatest(context map[string]interface{}) (downloadBytes int64, index *entity.Index, err error) {
	downloadCloudLatestLock.Lock()
	defer downloadCloudLatestLock.Unlock()

	start := time.Now()
	index = &entity.Index{}

	key := path.Join("refs", "latest")
	eventbus.Publish(eventbus.EvtCloudBeforeDownloadRef, context, "refs/latest")
	data, err := repo.downloadCloudObject(key)
	if nil != err {
		if errors.Is(err, cloud.ErrCloudObjectNotFound) {
			logging.LogWarnf("not found cloud latest")
			err = nil
			return
		}

		logging.LogErrorf("download cloud latest failed: %s", err)
		return
	}

	latestID := strings.TrimSpace(string(data))
	if 40 != len(latestID) {
		err = cloud.ErrCloudObjectNotFound
		logging.LogWarnf("got empty cloud latest")
		return
	}

	isS3OrSiYuan := repo.isCloudS3() || repo.isCloudSiYuan()
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		downloadBytes, index, err = repo.downloadCloudIndex(latestID, context)
	}()

	var seqNumLatestID string
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		if isS3OrSiYuan {
			// 确认下载到的是最新索引 https://github.com/siyuan-note/siyuan/issues/12991
			seqNumLatestID, _, _ = repo.getSeqNumLatest()
		}
	}()
	waitGroup.Wait()

	if isS3OrSiYuan && ("" != seqNumLatestID && "" != index.ID && latestID != seqNumLatestID) {
		logging.LogWarnf("cloud latest [%s] not match seq num latest [%s]", latestID, seqNumLatestID)
		// 以时间较新的为准
		_, seqNumLatest, downloadErr := repo.downloadCloudIndex(seqNumLatestID, context)
		if nil != downloadErr {
			logging.LogWarnf("download seq num latest [%s] failed: %s", seqNumLatestID, downloadErr)
		} else {
			if seqNumLatest.Created > index.Created {
				logging.LogWarnf("use seq num latest [%s] instead of cloud latest [%s]", seqNumLatest, index)
				index = seqNumLatest
			} else {
				logging.LogWarnf("still use cloud latest [%s] rather than seq num latest [%s]", index, seqNumLatest)
			}
		}
	}

	if !index.VerifyAESKey(repo.store.AesKey) {
		err = cloud.ErrDecryptFailed
		logging.LogErrorf("cloud latest [%s] verify AES key failed", index.String())
		return
	}

	logging.LogInfof("got cloud latest [%s], cost [%s]", index.String(), time.Since(start))
	return
}

func (repo *Repo) getSeqNumLatest() (id string, maxSeqNum int, seqNumLatests []string) {
	refs, listErr := repo.cloud.ListObjects("refs/")
	if nil != listErr {
		logging.LogErrorf("list refs failed: %s", listErr)
		return
	}
	for _, ref := range refs {
		if !strings.HasPrefix(ref.Path, "latest-") {
			continue
		}

		p := strings.TrimPrefix(ref.Path, "latest-")
		parts := strings.Split(p, "-")
		if 2 > len(parts) {
			repo.cloud.RemoveObject("refs/" + ref.Path)
			continue
		}

		seqNum, _ := strconv.Atoi(parts[0])
		if seqNum > maxSeqNum {
			maxSeqNum = seqNum
			id = parts[1]
		}

		seqNumLatests = append(seqNumLatests, "refs/"+ref.Path)
	}
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

	downloadStat, downloadErr := repo.downloadCloudChunksPut(chunkIDs, context)
	err = downloadErr
	if nil != err {
		return
	}
	stat.DownloadBytes = downloadStat.CloudBytes
	stat.DownloadChunkCount += len(chunkIDs)
	stat.PeerDownloadBytes += downloadStat.PeerBytes
	stat.PeerDownloadChunkCount += downloadStat.PeerCount
	stat.PeerFallbackCount += downloadStat.PeerFallbackCount

	err = repo.checkoutFiles(files, context)
	return
}

func (repo *Repo) RemoveCloudRepo(name string) (err error) {
	lock.Lock()
	defer lock.Unlock()

	context := map[string]interface{}{eventbus.CtxPushMsg: eventbus.CtxPushMsgToStatusBar}
	err = repo.tryLockCloud("remove", context)
	if nil != err {
		return
	}
	defer repo.unlockCloud(context)

	return repo.cloud.RemoveRepo(name)
}

func (repo *Repo) CreateCloudRepo(name string) (err error) {
	lock.Lock()
	defer lock.Unlock()

	context := map[string]interface{}{eventbus.CtxPushMsg: eventbus.CtxPushMsgToStatusBar}
	err = repo.tryLockCloud("create", context)
	if nil != err {
		return
	}
	defer repo.unlockCloud(context)

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
