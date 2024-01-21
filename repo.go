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
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/88250/gulu"
	"github.com/panjf2000/ants/v2"
	"github.com/restic/chunker"
	ignore "github.com/sabhiram/go-gitignore"
	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
	"github.com/siyuan-note/eventbus"
	"github.com/siyuan-note/filelock"
	"github.com/siyuan-note/logging"
)

// Repo 描述了逮虾户数据仓库。
type Repo struct {
	DataPath    string   // 数据文件夹的绝对路径，如：F:\\SiYuan\\data\\
	Path        string   // 仓库的绝对路径，如：F:\\SiYuan\\repo\\
	HistoryPath string   // 数据历史文件夹的绝对路径，如：F:\\SiYuan\\history\\
	TempPath    string   // 临时文件夹的绝对路径，如：F:\\SiYuan\\temp\\
	DeviceID    string   // 设备 ID
	DeviceName  string   // 设备名称
	DeviceOS    string   // 操作系统
	IgnoreLines []string // 忽略配置文件内容行，是用 .gitignore 语法

	store    *Store      // 仓库的存储
	chunkPol chunker.Pol // 文件分块多项式值
	cloud    cloud.Cloud // 云端存储服务
}

// NewRepo 创建一个新的仓库。
func NewRepo(dataPath, repoPath, historyPath, tempPath, deviceID, deviceName, deviceOS string, aesKey []byte, ignoreLines []string, cloud cloud.Cloud) (ret *Repo, err error) {
	if nil != cloud {
		cloud.GetConf().RepoPath = repoPath
	}
	ret = &Repo{
		DataPath:    filepath.Clean(dataPath),
		Path:        filepath.Clean(repoPath),
		HistoryPath: filepath.Clean(historyPath),
		TempPath:    filepath.Clean(tempPath),
		DeviceID:    deviceID,
		DeviceName:  deviceName,
		DeviceOS:    deviceOS,
		cloud:       cloud,
		chunkPol:    chunker.Pol(0x3DA3358B4DC173), // 固定分块多项式值
	}
	if !strings.HasSuffix(ret.DataPath, string(os.PathSeparator)) {
		ret.DataPath += string(os.PathSeparator)
	}
	if !strings.HasSuffix(ret.Path, string(os.PathSeparator)) {
		ret.Path += string(os.PathSeparator)
	}
	if !strings.HasSuffix(ret.HistoryPath, string(os.PathSeparator)) {
		ret.HistoryPath += string(os.PathSeparator)
	}
	ignoreLines = gulu.Str.RemoveDuplicatedElem(ignoreLines)
	ret.IgnoreLines = ignoreLines
	ret.store, err = NewStore(ret.Path, aesKey)
	return
}

var (
	ErrRepoFatal  = errors.New("repo fatal error")
	ErrEmptyIndex = errors.New("empty index")
	// ErrIndexFileChanged indicates that the file has changed during the index process.
	// Improve data snapshot and sync robustness https://github.com/siyuan-note/siyuan/issues/9941
	ErrIndexFileChanged = errors.New("file changed")
)

var lock = sync.Mutex{} // 仓库锁，Checkout、Index 和 Sync 等不能同时执行

func (repo *Repo) CountIndexes() (ret int, err error) {
	dir := filepath.Join(repo.Path, "indexes")
	files, err := os.ReadDir(dir)
	if nil != err {
		logging.LogErrorf("read dir [%s] failed: %s", dir, err)
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		id := file.Name()
		if 40 == len(id) {
			ret++
		}
	}
	return
}

// Reset 重置仓库，清空所有数据。
func (repo *Repo) Reset() (err error) {
	lock.Lock()
	defer lock.Unlock()

	if err = os.RemoveAll(repo.Path); nil != err {
		return
	}
	if err = os.MkdirAll(repo.Path, 0755); nil != err {
		return
	}
	return
}

// Purge 清理所有未引用数据。
func (repo *Repo) Purge() (ret *entity.PurgeStat, err error) {
	lock.Lock()
	defer lock.Unlock()
	return repo.store.Purge()
}

// PurgeCloud 清理云端所有未引用数据。
// Support manual purge of unreferenced data snapshots in the S3/WebDAV cloud storage https://github.com/siyuan-note/siyuan/issues/10081
func (repo *Repo) PurgeCloud() (ret *entity.PurgeStat, err error) {
	lock.Lock()
	defer lock.Unlock()

	context := map[string]interface{}{eventbus.CtxPushMsg: eventbus.CtxPushMsgToNone}
	err = repo.tryLockCloud("purge", context)
	if nil != err {
		return
	}
	defer repo.unlockCloud(context)

	logging.LogInfof("purging cloud...")
	objInfos, listErr := repo.cloud.ListObjects("objects/")
	if nil != listErr {
		logging.LogErrorf("list objects failed: %s", listErr)
		err = listErr
		return
	}

	objIDs := map[string]bool{}
	for objPath, _ := range objInfos {
		objID := strings.ReplaceAll(objPath, "/", "")
		objIDs[objID] = true
	}

	indexIDs, listErr := repo.cloud.ListObjects("indexes/")
	if nil != listErr {
		logging.LogErrorf("list indexes failed: %s", listErr)
		err = listErr
		return
	}

	if 1 > len(indexIDs) || 1 > len(objIDs) {
		logging.LogInfof("skip purge cloud")
		return
	}

	refs, listErr := repo.cloud.ListObjects("refs/")
	if nil != listErr {
		logging.LogErrorf("list refs failed: %s", listErr)
		err = listErr
		return
	}

	refIndexIDs := map[string]bool{}
	for r := range refs {
		ref, getErr := repo.cloud.DownloadObject(path.Join("refs", r))
		if nil != getErr {
			err = getErr
			logging.LogErrorf("get ref [%s] failed: %s", r, err)
			return
		}

		refID := strings.TrimSpace(string(ref))
		refIndexIDs[refID] = true
	}

	unreferencedIndexIDs := map[string]bool{}
	for indexID := range indexIDs {
		if !refIndexIDs[indexID] {
			unreferencedIndexIDs[indexID] = true
		}
	}

	referencedFileIDs := map[string]bool{}
	referencedObjIDs := map[string]bool{}
	for refID := range refIndexIDs {
		index, getErr := repo.cloud.GetIndex(refID)
		if nil != getErr {
			err = getErr
			logging.LogErrorf("get index [%s] failed: %s", refID, err)
			return
		}

		for _, fileID := range index.Files {
			referencedObjIDs[fileID] = true
			referencedFileIDs[fileID] = true
		}
	}

	var files []*entity.File
	var filesIDs []string
	for fileID := range referencedFileIDs {
		f, _ := repo.GetFile(fileID)
		if nil != f {
			files = append(files, f)
			continue
		}

		filesIDs = append(filesIDs, fileID)
	}

	_, dFiles, downloadErr := repo.downloadCloudFilesPut(filesIDs, context)
	if nil != downloadErr {
		err = downloadErr
		logging.LogErrorf("download cloud files failed: %s", err)
		return
	}
	files = append(files, dFiles...)

	for _, f := range files {
		for _, chunkID := range f.Chunks {
			referencedObjIDs[chunkID] = true
		}
	}

	unreferencedIDs := map[string]bool{}
	for objID := range objIDs {
		if !referencedObjIDs[objID] {
			unreferencedIDs[objID] = true
		}
	}

	ret = &entity.PurgeStat{}
	ret.Indexes = len(unreferencedIndexIDs)

	unreferencedPaths := []string{}
	for unreferencedID := range unreferencedIDs {
		unreferencedPath := path.Join(unreferencedID[:2], unreferencedID[2:])
		objInfo := objInfos[unreferencedPath]
		if nil == objInfo {
			logging.LogWarnf("unreferenced object [%s] not found", unreferencedPath)
			continue
		}

		ret.Size += objInfo.Size
		ret.Objects++

		unreferencedPaths = append(unreferencedPaths, unreferencedPath)
	}
	unreferencedPaths = gulu.Str.RemoveDuplicatedElem(unreferencedPaths)

	// 删除所有遗留的校验索引
	// S3/WebDAV 不上传校验索引 S3/WebDAV data sync no longer uploads check index https://github.com/siyuan-note/siyuan/issues/10180
	checkIndexIDs, listErr := repo.cloud.ListObjects("check/indexes/")
	if nil != listErr {
		logging.LogErrorf("list indexes failed: %s", listErr)
		err = listErr
		return
	}

	var unreferencedCheckIndexPaths []string
	for checkIndexID := range checkIndexIDs {
		checkIndexPath := path.Join("check", "indexes", checkIndexID)
		unreferencedCheckIndexPaths = append(unreferencedCheckIndexPaths, checkIndexPath)
	}
	err = repo.removeCloudObjects(unreferencedCheckIndexPaths)
	if nil != err {
		logging.LogErrorf("remove unreferenced check indexes failed: %s", err)
		return
	}

	// 删除索引
	var unreferencedIndexPaths []string
	for unreferencedID := range unreferencedIndexIDs {
		indexPath := path.Join("indexes", unreferencedID)
		unreferencedIndexPaths = append(unreferencedIndexPaths, indexPath)
	}

	err = repo.removeCloudObjects(unreferencedIndexPaths)
	if nil != err {
		logging.LogErrorf("remove unreferenced indexes failed: %s", err)
		return
	}

	// 清理索引列表
	err = repo.purgeIndexesV2(refIndexIDs)
	if nil != err {
		logging.LogErrorf("purge indexes-v2.json failed: %s", err)
		return
	}

	// 删除对象
	var unreferencedObjPaths []string
	for _, unreferencedPath := range unreferencedPaths {
		objPath := path.Join("objects", unreferencedPath)
		unreferencedObjPaths = append(unreferencedObjPaths, objPath)
	}
	err = repo.removeCloudObjects(unreferencedObjPaths)
	if nil != err {
		logging.LogErrorf("remove unreferenced objects failed: %s", err)
		return
	}

	logging.LogInfof("purged cloud, [%d] indexes, [%d] objects, [%d] bytes", ret.Indexes, ret.Objects, ret.Size)
	return
}

func (repo *Repo) purgeIndexesV2(refIndexIDs map[string]bool) (err error) {
	data, err := repo.cloud.DownloadObject("indexes-v2.json")
	if nil != err {
		if !errors.Is(err, cloud.ErrCloudObjectNotFound) {
			return
		}
		err = nil
		return
	}

	data, err = repo.store.compressDecoder.DecodeAll(data, nil)
	if nil != err {
		return
	}

	indexes := &cloud.Indexes{}
	if 0 < len(data) {
		if err = gulu.JSON.UnmarshalJSON(data, &indexes); nil != err {
			logging.LogWarnf("unmarshal cloud indexes-v2.json failed: %s", err)
		}
	}

	var tmp []*cloud.Index
	for _, index := range indexes.Indexes {
		if refIndexIDs[index.ID] {
			tmp = append(tmp, index)
		}
	}
	indexes.Indexes = tmp

	if data, err = gulu.JSON.MarshalIndentJSON(indexes, "", "\t"); nil != err {
		return
	}

	data = repo.store.compressEncoder.EncodeAll(data, nil)

	if err = gulu.File.WriteFileSafer(filepath.Join(repo.Path, "indexes-v2.json"), data, 0644); nil != err {
		return
	}

	_, err = repo.cloud.UploadObject("indexes-v2.json", true)
	return
}

// GetIndex 从仓库根据 id 获取索引。
func (repo *Repo) GetIndex(id string) (index *entity.Index, err error) {
	lock.Lock()
	defer lock.Unlock()
	return repo.store.GetIndex(id)
}

// PutIndex 将索引 index 写入仓库。
func (repo *Repo) PutIndex(index *entity.Index) (err error) {
	lock.Lock()
	defer lock.Unlock()
	return repo.store.PutIndex(index)
}

var workspaceDataDirs = []string{"assets", "emojis", "snippets", "storage", "templates", "widgets"}
var removeEmptyDirExcludes = append(workspaceDataDirs, ".git")

// Checkout 将仓库中的数据迁出到 repo 数据文件夹下。context 参数用于发布事件时传递调用上下文。
func (repo *Repo) Checkout(id string, context map[string]interface{}) (upserts, removes []*entity.File, err error) {
	lock.Lock()
	defer lock.Unlock()

	index, err := repo.store.GetIndex(id)
	if nil != err {
		return
	}

	if err = os.MkdirAll(repo.DataPath, 0755); nil != err {
		return
	}
	var files []*entity.File
	ignoreMatcher := repo.ignoreMatcher()
	eventbus.Publish(eventbus.EvtCheckoutBeforeWalkData, context, repo.DataPath)
	err = filelock.Walk(repo.DataPath, func(path string, info os.FileInfo, err error) error {
		if nil != err {
			logging.LogErrorf("walk data failed: %s", err)
			return err
		}
		if ignored, ignoreResult := repo.builtInIgnore(info, path); ignored || nil != ignoreResult {
			return ignoreResult
		}

		p := repo.relPath(path)
		if ignoreMatcher.MatchesPath(p) {
			return nil
		}

		files = append(files, entity.NewFile(p, info.Size(), info.ModTime().UnixMilli()))
		eventbus.Publish(eventbus.EvtCheckoutWalkData, context, p)
		return nil
	})
	if nil != err {
		return
	}

	defer gulu.File.RemoveEmptyDirs(repo.DataPath, removeEmptyDirExcludes...)

	latestFiles, err := repo.getFiles(index.Files)
	if nil != err {
		return
	}

	upserts, removes = repo.diffUpsertRemove(latestFiles, files, false, false)
	if 1 > len(upserts) && 1 > len(removes) {
		return
	}

	count, total := 0, len(upserts)
	eventbus.Publish(eventbus.EvtCheckoutUpsertFiles, context, total)
	for _, file := range upserts {
		count++
		if err = repo.checkoutFile(file, repo.DataPath, count, total, context); nil != err {
			return
		}
	}

	total = len(removes)
	eventbus.Publish(eventbus.EvtCheckoutRemoveFiles, context, total)
	for i, f := range removes {
		absPath := repo.absPath(f.Path)
		if err = filelock.Remove(absPath); nil != err {
			return
		}
		eventbus.Publish(eventbus.EvtCheckoutRemoveFile, context, i+1, total)
	}
	return
}

// Index 将 repo 数据文件夹中的文件索引到仓库中。context 参数用于发布事件时传递调用上下文。
func (repo *Repo) Index(memo string, context map[string]interface{}) (ret *entity.Index, err error) {
	lock.Lock()
	defer lock.Unlock()

	ret, err = repo.index(memo, context)
	return
}

// GetFiles 返回快照索引 index 中的文件列表。
func (repo *Repo) GetFiles(index *entity.Index) (ret []*entity.File, err error) {
	ret, err = repo.getFiles(index.Files)
	return
}

func (repo *Repo) GetFile(fileID string) (ret *entity.File, err error) {
	ret, err = repo.store.GetFile(fileID)
	return
}

func (repo *Repo) OpenFile(file *entity.File) (ret []byte, err error) {
	ret, err = repo.openFile(file)
	return
}

func (repo *Repo) removeCloudObjects(objects []string) (err error) {
	waitGroup := &sync.WaitGroup{}
	var removeErr error
	poolSize := 8
	if poolSize > len(objects) {
		poolSize = len(objects)
	}

	p, err := ants.NewPoolWithFunc(poolSize, func(arg interface{}) {
		defer waitGroup.Done()
		if nil != removeErr {
			return // 快速失败
		}

		fileID := arg.(string)
		rmErr := repo.cloud.RemoveObject(fileID)
		if nil != rmErr {
			removeErr = rmErr
			return
		}
	})
	if nil != err {
		return
	}

	for _, obj := range objects {
		waitGroup.Add(1)
		if err = p.Invoke(obj); nil != err {
			logging.LogErrorf("invoke failed: %s", err)
			return
		}
		if nil != removeErr {
			err = removeErr
			return
		}
	}
	waitGroup.Wait()
	p.Release()
	if nil != removeErr {
		err = removeErr
		return
	}
	return
}

func (repo *Repo) index(memo string, context map[string]interface{}) (ret *entity.Index, err error) {
	for i := 0; i < 7; i++ {
		ret, err = repo.index0(memo, context)
		if nil == err {
			return
		}

		if !errors.Is(err, ErrIndexFileChanged) {
			return
		}

		logging.LogWarnf("index failed, caused by: %s, retrying [%d]", err, i)
	}

	logging.LogWarnf("index failed after 7 retries, caused by: %s", err)
	return
}

func (repo *Repo) index0(memo string, context map[string]interface{}) (ret *entity.Index, err error) {
	var files []*entity.File
	ignoreMatcher := repo.ignoreMatcher()
	eventbus.Publish(eventbus.EvtIndexBeforeWalkData, context, repo.DataPath)
	//start := time.Now()
	err = filelock.Walk(repo.DataPath, func(path string, info os.FileInfo, err error) error {
		if nil != err {
			if isNoSuchFileOrDirErr(err) {
				// An error `Failed to create data snapshot` is occasionally reported during automatic data sync https://github.com/siyuan-note/siyuan/issues/8998
				logging.LogInfof("ignore not exist err [%s]", err)
				return nil
			}
			logging.LogErrorf("walk data failed: %s", err)
			return err
		}
		if ignored, ignoreErr := repo.builtInIgnore(info, path); ignored || nil != ignoreErr {
			return ignoreErr
		}

		p := repo.relPath(path)
		if ignoreMatcher.MatchesPath(p) {
			return nil
		}

		files = append(files, entity.NewFile(p, info.Size(), info.ModTime().UnixMilli()))
		eventbus.Publish(eventbus.EvtIndexWalkData, context, p)
		return nil
	})
	if nil != err {
		logging.LogErrorf("walk data failed: %s", err)
		return
	}
	//logging.LogInfof("walk data [files=%d] cost [%s]", len(files), time.Since(start))
	//sort.Slice(files, func(i, j int) bool { return files[i].Updated > files[j].Updated })
	//for _, f := range files {
	//	logging.LogInfof("walked data [file=%s]", f.Path)
	//}
	if 1 > len(files) {
		// 如果没有文件，则不创建快照 Abandon snapshot if file does not exist when creating snapshot https://github.com/siyuan-note/siyuan/issues/9948
		err = ErrEmptyIndex
		logging.LogErrorf("empty index [%s]", repo.DataPath)
		return
	}

	latest, err := repo.Latest()
	init := false
	if nil != err {
		if ErrNotFoundIndex != err {
			logging.LogErrorf("get latest index failed: %s", err)
			return
		}

		// 如果没有索引，则创建第一个索引
		latest = &entity.Index{
			ID:         util.RandHash(),
			Memo:       memo,
			Created:    time.Now().UnixMilli(),
			SystemID:   repo.DeviceID,
			SystemName: repo.DeviceName,
			SystemOS:   repo.DeviceOS,
		}
		init = true
	}

	var workerErrs []error
	workerErrLock := sync.Mutex{}
	var upserts, removes, latestFiles []*entity.File
	if !init {
		count := atomic.Int32{}
		total := len(files)
		eventbus.Publish(eventbus.EvtIndexBeforeGetLatestFiles, context, total)
		lock := &sync.Mutex{}
		waitGroup := &sync.WaitGroup{}
		p, _ := ants.NewPoolWithFunc(4, func(arg interface{}) {
			defer waitGroup.Done()

			count.Add(1)
			eventbus.Publish(eventbus.EvtIndexGetLatestFile, context, int(count.Load()), total)

			fileID := arg.(string)
			file, getErr := repo.store.GetFile(fileID)
			if nil != getErr {
				logging.LogErrorf("get file [%s] failed: %s", fileID, getErr)
				workerErrLock.Lock()
				workerErrs = append(workerErrs, ErrNotFoundObject)
				workerErrLock.Unlock()
				return
			}

			lock.Lock()
			latestFiles = append(latestFiles, file)
			lock.Unlock()

			// Check local data chunk integrity before data synchronization https://github.com/siyuan-note/siyuan/issues/8853
			for _, chunk := range file.Chunks {
				info, statErr := repo.store.Stat(chunk)
				if nil == statErr {
					continue
				}

				if nil != info {
					logging.LogWarnf("stat file [%s, %s, %s, %d] chunk [%s, perm=%04o] failed: %s",
						file.ID, file.Path, time.UnixMilli(file.Updated).Format("2006-01-02 15:04:05"), file.Size, chunk, info.Mode().Perm(), statErr)
				} else {
					logging.LogWarnf("stat file [%s, %s, %s, %d] chunk [%s] failed: %s",
						file.ID, file.Path, time.UnixMilli(file.Updated).Format("2006-01-02 15:04:05"), file.Size, chunk, statErr)
				}

				if errors.Is(statErr, os.ErrPermission) {
					// 如果是权限问题，则尝试修改权限，不认为是分块文件损坏
					// Improve checking local data chunk integrity before data sync https://github.com/siyuan-note/siyuan/issues/9688
					if chmodErr := os.Chmod(chunk, 0644); nil != chmodErr {
						logging.LogWarnf("chmod file [%s] failed: %s", chunk, chmodErr)
					} else {
						logging.LogInfof("chmod file [%s] to [0644]", chunk)
					}
					continue
				}

				if errors.Is(statErr, os.ErrNotExist) {
					workerErrLock.Lock()
					workerErrs = append(workerErrs, ErrNotFoundObject)
					workerErrLock.Unlock()
					return
				}
			}
		})

		for _, f := range latest.Files {
			waitGroup.Add(1)
			err = p.Invoke(f)
			if nil != err {
				logging.LogErrorf("invoke failed: %s", err)
				return
			}
		}
		waitGroup.Wait()
		p.Release()

		if 0 < len(workerErrs) {
			err = workerErrs[0]
			logging.LogErrorf("get latest files failed: %s", err)
			return
		}
	}
	upserts, removes = repo.diffUpsertRemove(files, latestFiles, false, false)
	if 1 > len(upserts) && 1 > len(removes) {
		ret = latest
		return
	}

	if init {
		ret = latest
	} else {
		ret = &entity.Index{
			ID:         util.RandHash(),
			Memo:       memo,
			Created:    time.Now().UnixMilli(),
			SystemID:   repo.DeviceID,
			SystemName: repo.DeviceName,
			SystemOS:   repo.DeviceOS,
		}
	}

	count := atomic.Int32{}
	total := len(upserts)
	workerErrs = nil
	eventbus.Publish(eventbus.EvtIndexUpsertFiles, context, total)
	waitGroup := &sync.WaitGroup{}
	p, _ := ants.NewPoolWithFunc(4, func(arg interface{}) {
		defer waitGroup.Done()

		count.Add(1)
		file := arg.(*entity.File)
		putErr := repo.putFileChunks(file, context, int(count.Load()), total)
		if nil != putErr {
			workerErrLock.Lock()
			workerErrs = append(workerErrs, putErr)
			workerErrLock.Unlock()
			return
		}

		if 1 > len(file.Chunks) {
			workerErrLock.Lock()
			putErr = fmt.Errorf("file [%s, %s, %s, %d] has no chunks", file.ID, file.Path, time.UnixMilli(file.Updated).Format("2006-01-02 15:04:05"), file.Size)
			workerErrs = append(workerErrs, putErr)
			workerErrLock.Unlock()
			return
		}
	})

	for _, file := range upserts {
		waitGroup.Add(1)
		err = p.Invoke(file)
		if nil != err {
			logging.LogErrorf("invoke failed: %s", err)
			return
		}
	}
	waitGroup.Wait()
	p.Release()

	if 0 < len(workerErrs) {
		for _, e := range workerErrs {
			if errors.Is(e, ErrIndexFileChanged) {
				err = e
				return
			}
		}

		err = workerErrs[0]
		logging.LogErrorf("put file chunks failed: %s", err)
		return
	}

	for _, file := range files {
		ret.Files = append(ret.Files, file.ID)
		ret.Size += file.Size
	}
	ret.Count = len(ret.Files)

	err = repo.store.PutIndex(ret)
	if nil != err {
		logging.LogErrorf("put index failed: %s", err)
		return
	}

	err = repo.UpdateLatest(ret)
	if nil != err {
		logging.LogErrorf("update latest failed: %s", err)
		return
	}
	return
}

func (repo *Repo) builtInIgnore(info os.FileInfo, absPath string) (ignored bool, err error) {
	name := info.Name()
	if info.IsDir() {
		if strings.HasPrefix(name, ".") {
			if ".siyuan" == name {
				return true, nil
			}
			return true, filepath.SkipDir
		}
		if "filesys_status_check" == name {
			// 数据同步忽略用于文件系统检查的文件 https://github.com/siyuan-note/siyuan/issues/7744
			return true, filepath.SkipDir
		}
		return true, nil
	} else {
		if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".tmp") {
			return true, nil
		}

		slashAbsPath := filepath.ToSlash(absPath)
		if strings.HasSuffix(slashAbsPath, "data/storage/local.json") {
			// localStorage 不再支持同步 https://github.com/siyuan-note/siyuan/issues/6964
			return true, nil
		}
		if strings.HasSuffix(slashAbsPath, "data/storage/recent-doc.json") {
			// 数据同步忽略最近文档存储 https://github.com/siyuan-note/siyuan/issues/7246
			return true, nil
		}
	}

	if gulu.File.IsHidden(absPath) {
		return true, nil
	}

	if !info.Mode().IsRegular() {
		return true, nil
	}
	return false, nil
}

func (repo *Repo) ignoreMatcher() *ignore.GitIgnore {
	return ignore.CompileIgnoreLines(repo.IgnoreLines...)
}

func (repo *Repo) absPath(relPath string) string {
	return filepath.Join(repo.DataPath, relPath)
}

func (repo *Repo) relPath(absPath string) string {
	absPath = filepath.Clean(absPath)
	return "/" + filepath.ToSlash(strings.TrimPrefix(absPath, repo.DataPath))
}

func (repo *Repo) putFileChunks(file *entity.File, context map[string]interface{}, count, total int) (err error) {
	absPath := repo.absPath(file.Path)

	if chunker.MinSize > file.Size {
		var data []byte
		data, err = filelock.ReadFile(absPath)
		if nil != err {
			logging.LogErrorf("read file [%s] failed: %s", absPath, err)
			return
		}

		chunkHash := util.Hash(data)
		file.Chunks = append(file.Chunks, chunkHash)
		chunk := &entity.Chunk{ID: chunkHash, Data: data}
		if err = repo.store.PutChunk(chunk); nil != err {
			logging.LogErrorf("put chunk [%s] failed: %s", chunkHash, err)
			return
		}

		newInfo, statErr := os.Stat(absPath)
		if nil != statErr {
			logging.LogErrorf("stat file [%s] failed: %s", absPath, statErr)
			err = statErr
			return
		}

		newSize := newInfo.Size()
		newUpdated := newInfo.ModTime().Unix()
		if file.Size != newSize || file.SecUpdated() != newUpdated {
			logging.LogErrorf("file changed [%s], size [%d -> %d], updated [%d -> %d]", absPath, file.Size, newSize, file.SecUpdated(), newUpdated)
			err = ErrIndexFileChanged
			return
		}

		eventbus.Publish(eventbus.EvtIndexUpsertFile, context, count, total)
		err = repo.store.PutFile(file)
		if nil != err {
			return
		}
		return
	}

	reader, err := filelock.OpenFile(absPath, os.O_RDONLY, 0644)
	if nil != err {
		logging.LogErrorf("open file [%s] failed: %s", absPath, err)
		return
	}

	chnkr := chunker.NewWithBoundaries(reader, repo.chunkPol, chunker.MinSize, chunker.MaxSize)
	for {
		buf := make([]byte, chunker.MaxSize)
		chnk, chnkErr := chnkr.Next(buf)
		if io.EOF == chnkErr {
			break
		}
		if nil != chnkErr {
			err = chnkErr
			logging.LogErrorf("chunk file [%s] failed: %s", absPath, chnkErr)
			if closeErr := filelock.CloseFile(reader); nil != closeErr {
				logging.LogErrorf("close file [%s] failed: %s", absPath, closeErr)
			}
			return
		}

		chunkHash := util.Hash(chnk.Data)
		file.Chunks = append(file.Chunks, chunkHash)
		chunk := &entity.Chunk{ID: chunkHash, Data: chnk.Data}
		if err = repo.store.PutChunk(chunk); nil != err {
			logging.LogErrorf("put chunk [%s] failed: %s", chunkHash, err)
			if closeErr := filelock.CloseFile(reader); nil != closeErr {
				logging.LogErrorf("close file [%s] failed: %s", absPath, closeErr)
			}
			return
		}
	}

	if err = filelock.CloseFile(reader); nil != err {
		logging.LogErrorf("close file [%s] failed: %s", absPath, err)
		return
	}

	newInfo, statErr := os.Stat(absPath)
	if nil != statErr {
		logging.LogErrorf("stat file [%s] failed: %s", absPath, statErr)
		err = statErr
		return
	}

	newSize := newInfo.Size()
	newUpdated := newInfo.ModTime().UnixMilli()
	if file.Size != newSize || file.Updated != newUpdated {
		logging.LogErrorf("file changed [%s], size [%d -> %d], updated [%d -> %d]", absPath, file.Size, newSize, file.Updated, newUpdated)
		err = ErrIndexFileChanged
		return
	}

	eventbus.Publish(eventbus.EvtIndexUpsertFile, context, count, total)
	err = repo.store.PutFile(file)
	return
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

func (repo *Repo) openFile(file *entity.File) (ret []byte, err error) {
	for _, c := range file.Chunks {
		var chunk *entity.Chunk
		chunk, err = repo.store.GetChunk(c)
		if nil != err {
			return
		}
		ret = append(ret, chunk.Data...)
	}
	return
}

//func (repo *Repo) checkoutFiles(files []*entity.File, context map[string]interface{}) (err error) {
//	now := time.Now()
//
//	total := len(files)
//	eventbus.Publish(eventbus.EvtCheckoutUpsertFiles, context, total)
//	for i, file := range files {
//		err = repo.checkoutFile(file, repo.DataPath, i+1, total, context)
//		if nil != err {
//			return
//		}
//	}
//
//	logging.LogInfof("checkout files done, total: %d, cost: %s", total, time.Since(now))
//	return
//}

func (repo *Repo) checkoutFiles(files []*entity.File, context map[string]interface{}) (err error) {
	//now := time.Now()

	var dotSiYuans, tmp []*entity.File
	for _, file := range files {
		if strings.Contains(file.Path, ".siyuan") {
			dotSiYuans = append(dotSiYuans, file)
		} else {
			tmp = append(tmp, file)
		}
	}
	sort.Slice(dotSiYuans, func(i, j int) bool {
		if strings.Contains(dotSiYuans[i].Path, "conf.json") {
			return true
		}
		if strings.Contains(dotSiYuans[j].Path, "conf.json") {
			return false
		}
		return dotSiYuans[i].Updated > dotSiYuans[j].Updated
	})
	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i].Updated > tmp[j].Updated
	})
	tmp = append(dotSiYuans, tmp...)
	files = tmp

	count := atomic.Int32{}
	total := len(files)
	eventbus.Publish(eventbus.EvtCheckoutUpsertFiles, context, total)
	waitGroup := &sync.WaitGroup{}
	p, _ := ants.NewPoolWithFunc(4, func(arg interface{}) {
		defer waitGroup.Done()

		file := arg.(*entity.File)
		count.Add(1)
		err = repo.checkoutFile(file, repo.DataPath, int(count.Load()), total, context)
		if nil != err {
			return
		}
	})

	for _, f := range files {
		waitGroup.Add(1)
		err = p.Invoke(f)
		if nil != err {
			logging.LogErrorf("invoke failed: %s", err)
			return
		}
	}
	waitGroup.Wait()
	p.Release()

	//logging.LogInfof("checkout files done, total: %d, cost: %s", total, time.Since(now))
	return
}

func (repo *Repo) checkoutFile(file *entity.File, checkoutDir string, count, total int, context map[string]interface{}) (err error) {
	absPath := filepath.Join(checkoutDir, file.Path)
	dir, name := filepath.Split(absPath)
	if err = os.MkdirAll(dir, 0755); nil != err {
		return
	}

	tmp := filepath.Join(dir, name+gulu.Rand.String(7)+".tmp")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if nil != err {
		return
	}

	for _, c := range file.Chunks {
		var chunk *entity.Chunk
		chunk, err = repo.store.GetChunk(c)
		if nil != err {
			return
		}

		if _, err = f.Write(chunk.Data); nil != err {
			logging.LogErrorf("write file [%s] failed: %s", absPath, err)
			return
		}
	}

	if err = f.Sync(); nil != err {
		logging.LogErrorf("write file [%s] failed: %s", absPath, err)
		return
	}
	if err = f.Close(); nil != err {
		logging.LogErrorf("write file [%s] failed: %s", absPath, err)
		return
	}

	filelock.Lock(absPath)
	defer filelock.Unlock(absPath)

	for i := 0; i < 3; i++ {
		err = os.Rename(f.Name(), absPath) // Windows 上重命名是非原子的
		if nil == err {
			os.Remove(f.Name())
			break
		}

		if errMsg := strings.ToLower(err.Error()); strings.Contains(errMsg, "access is denied") || strings.Contains(errMsg, "used by another process") { // 文件可能是被锁定
			time.Sleep(200 * time.Millisecond)
			continue
		}
		break
	}
	if nil != err {
		logging.LogFatalf(logging.ExitCodeFileSysErr, "write file [%s] failed: %s", absPath, err)
	}

	updated := time.UnixMilli(file.Updated)
	if err = os.Chtimes(absPath, updated, updated); nil != err {
		logging.LogErrorf("change [%s] time failed: %s", absPath, err)
		return
	}
	eventbus.Publish(eventbus.EvtCheckoutUpsertFile, context, count, total)
	return
}

func isNoSuchFileOrDirErr(err error) bool {
	if nil == err {
		return false
	}

	return os.IsNotExist(err) || strings.Contains(err.Error(), "no such file or directory")
}
