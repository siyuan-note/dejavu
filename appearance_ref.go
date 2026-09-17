package dejavu

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/cloud"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

const AppearanceRecoveryTag = ".siyuan-appearance-v1"

var ErrAppearanceRecoveryTag = errors.New("appearance recovery tag is managed internally")
var ErrAppearanceRecoveryTagConflict = errors.New("appearance recovery tag conflicts with a user tag")

// IsAppearanceRecoveryTag 仅识别保留名称，实际归属必须认证对应快照中的协议标记。
func IsAppearanceRecoveryTag(tag string) bool {
	return tag == AppearanceRecoveryTag
}

func newAppearanceTraffic() *TrafficStat {
	return &TrafficStat{m: &sync.Mutex{}}
}

func addAppearanceTraffic(target, source *TrafficStat) {
	if target == nil || source == nil {
		return
	}
	if target.m != nil {
		target.m.Lock()
		defer target.m.Unlock()
	}
	target.DownloadBytes += source.DownloadBytes
	target.DownloadFileCount += source.DownloadFileCount
	target.DownloadChunkCount += source.DownloadChunkCount
	target.PeerDownloadBytes += source.PeerDownloadBytes
	target.PeerDownloadFileCount += source.PeerDownloadFileCount
	target.PeerDownloadChunkCount += source.PeerDownloadChunkCount
	target.PeerFallbackCount += source.PeerFallbackCount
	target.UploadBytes += source.UploadBytes
	target.UploadFileCount += source.UploadFileCount
	target.UploadChunkCount += source.UploadChunkCount
	target.APIGet += source.APIGet
	target.APIPut += source.APIPut
}

func addAppearanceFileDownloads(target *TrafficStat, count int, source *chunkDownloadStat) {
	target.DownloadBytes += source.CloudBytes
	target.DownloadFileCount += count
	target.DownloadChunkCount += source.PrefetchedChunkCount
	target.PeerDownloadBytes += source.PeerBytes
	target.PeerDownloadFileCount += source.PeerCount
	target.PeerDownloadChunkCount += source.PrefetchedChunkCount
	target.PeerFallbackCount += source.PeerFallbackCount
	target.APIGet += count - source.PeerCount
}

func (repo *Repo) appearanceRootFiles(index *entity.Index, remote bool, context map[string]interface{}, stat *TrafficStat) ([]*entity.File, error) {
	missing, err := repo.localNotFoundFiles(index.Files)
	if err != nil {
		return nil, err
	}
	if len(missing) != 0 {
		if !remote {
			return nil, fmt.Errorf("%w: missing appearance recovery file metadata", ErrRepoFatal)
		}
		download, _, downloadErr := repo.downloadCloudFilesPut(missing, context)
		addAppearanceFileDownloads(stat, len(missing), download)
		if downloadErr != nil {
			return nil, downloadErr
		}
	}
	return repo.getFiles(index.Files)
}

func (repo *Repo) appearanceRootEnsureChunks(file *entity.File, remote bool, context map[string]interface{}, stat *TrafficStat) error {
	if !validAssetFile(file) {
		return fmt.Errorf("%w: invalid appearance recovery file metadata", ErrRepoFatal)
	}
	missing, err := repo.localNotFoundChunks(file.Chunks)
	if err != nil {
		return err
	}
	if len(missing) != 0 {
		if !remote {
			return fmt.Errorf("%w: missing appearance recovery file chunks", ErrRepoFatal)
		}
		download, downloadErr := repo.downloadCloudChunksPut(missing, context)
		stat.DownloadBytes += download.CloudBytes
		stat.DownloadChunkCount += len(missing)
		stat.PeerDownloadBytes += download.PeerBytes
		stat.PeerDownloadChunkCount += download.PeerCount
		stat.PeerFallbackCount += download.PeerFallbackCount
		stat.APIGet += download.CloudCount
		if downloadErr != nil {
			return downloadErr
		}
	}
	return nil
}

func (repo *Repo) appearanceRootChunks(file *entity.File, remote bool, context map[string]interface{}, stat *TrafficStat) error {
	if err := repo.appearanceRootEnsureChunks(file, remote, context, stat); err != nil {
		return err
	}
	var size int64
	for _, id := range file.Chunks {
		chunk, getErr := repo.store.GetChunk(id)
		if getErr != nil {
			return getErr
		}
		if util.Hash(chunk.Data) != id || int64(len(chunk.Data)) > file.Size-size {
			return fmt.Errorf("%w: appearance recovery chunk identity mismatch", ErrRepoFatal)
		}
		size += int64(len(chunk.Data))
	}
	if size != file.Size {
		return fmt.Errorf("%w: appearance recovery file size mismatch", ErrRepoFatal)
	}
	return nil
}

// appearanceOwnsIndex 只将精确认证标记视为内部归属，普通同名用户标签保持可见和可编辑。
func (repo *Repo) appearanceOwnsIndex(index *entity.Index, remote bool, context map[string]interface{}, stat *TrafficStat) (bool, error) {
	marker := entity.NewFile(appearanceFormatPath, int64(len(appearanceFormatData)), appearanceEventModified)
	found := false
	for _, id := range index.Files {
		if id == marker.ID {
			found = true
			break
		}
	}
	if !found {
		return false, nil
	}
	file := &entity.File{}
	var err error
	// 固定标记旧 ID 不承诺内容，归属必须认证读取被检查端的对象，不能借用本地或全局元数据缓存。
	if remote {
		var length int64
		length, file, err = repo.downloadCloudFile(marker.ID, 1, 1, context)
		stat.DownloadBytes += length
		stat.DownloadFileCount++
		stat.APIGet++
		if err != nil {
			return false, err
		}
	} else {
		_, abs := repo.store.AbsPath(marker.ID)
		metadata, readErr := os.ReadFile(abs)
		if readErr != nil {
			return false, readErr
		}
		if metadata, err = repo.store.decodeData(metadata); err != nil {
			return false, err
		}
		if err = gulu.JSON.UnmarshalJSON(metadata, file); err != nil {
			return false, err
		}
	}
	if file.ID != marker.ID || file.Path != marker.Path || file.Size != marker.Size || file.Updated != marker.Updated {
		return false, nil
	}
	if err = repo.appearanceRootChunks(file, remote, context, stat); err != nil {
		return false, err
	}
	data, err := repo.openFile(file)
	return bytes.Equal(data, []byte(appearanceFormatData)), err
}

func (repo *Repo) validateAppearanceRecoveryIndex(index *entity.Index, remote bool, context map[string]interface{}, stat *TrafficStat) error {
	owned, err := repo.appearanceOwnsIndex(index, remote, context, stat)
	if err != nil {
		return err
	}
	if !owned {
		return ErrAppearanceRecoveryTagConflict
	}
	files, err := repo.appearanceRootFiles(index, remote, context, stat)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, file := range files {
		if !strings.HasPrefix(file.Path, "/storage/appearance-v1/") || file.Path == appearanceFormatPath {
			continue
		}
		if !validAssetFile(file) || seen[file.Path] || file.ID != entity.NewFile(file.Path, file.Size, file.Updated).ID {
			return fmt.Errorf("%w: invalid appearance recovery event metadata", ErrRepoFatal)
		}
		seen[file.Path] = true
		if repo.appearanceProtocolFileIgnored(file.Path) {
			continue
		}
		if key, _ := appearanceArchiveKey(file.Path); key == "" {
			return fmt.Errorf("%w: unknown appearance recovery event path", ErrRepoFatal)
		}
		if err = repo.appearanceRootEnsureChunks(file, remote, context, stat); err != nil {
			return err
		}
		if _, err = repo.readAppearanceStoredArchive(file, context, false); err != nil {
			return err
		}
	}
	events, err := repo.readAppearanceEvents(files, context)
	if err != nil {
		return err
	}
	_, err = unionAppearanceEvents(events)
	return err
}

func (repo *Repo) loadAppearanceTagIndex(remote bool, context map[string]interface{}, stat *TrafficStat) (*entity.Index, error) {
	key := path.Join("refs", "tags", AppearanceRecoveryTag)
	var data []byte
	var err error
	if remote {
		if repo.cloud == nil {
			return nil, nil
		}
		stat.APIGet++
		stat.DownloadFileCount++
		data, err = repo.cloud.DownloadObject(key)
		stat.DownloadBytes += int64(len(data))
		if errors.Is(err, cloud.ErrCloudObjectNotFound) {
			return nil, nil
		}
	} else {
		data, err = os.ReadFile(filepath.Join(repo.Path, filepath.FromSlash(key)))
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
	}
	if err != nil {
		return nil, err
	}
	id := string(data)
	if !validSnapshotID(id) {
		return nil, fmt.Errorf("%w: invalid appearance recovery reference", ErrRepoFatal)
	}
	index, err := repo.store.GetIndex(id)
	if errors.Is(err, os.ErrNotExist) && remote {
		var length int64
		length, index, err = repo.downloadCloudIndex(id, context)
		stat.DownloadBytes += length
		stat.DownloadFileCount++
		stat.APIGet++
		if err == nil {
			err = repo.store.PutIndex(index)
		}
	}
	if err != nil {
		return nil, err
	}
	if index.ID != id || !index.VerifyAESKey(repo.store.AesKey) {
		return nil, fmt.Errorf("%w: invalid appearance recovery index identity", ErrRepoFatal)
	}
	return index, nil
}

// readAppearanceRecoveryRoot 由已持有仓库锁和云端锁的同步过程读取并认证完整恢复快照。
func (repo *Repo) readAppearanceRecoveryRoot(context map[string]interface{}) (*entity.Index, *TrafficStat, error) {
	stat := newAppearanceTraffic()
	if !repo.appearanceSyncEnabled {
		return nil, stat, nil
	}
	index, err := repo.loadAppearanceTagIndex(true, context, stat)
	if err != nil || index == nil {
		return index, stat, err
	}
	if err = repo.validateAppearanceRecoveryIndex(index, true, context, stat); err != nil {
		return nil, stat, err
	}
	return index, stat, nil
}

func (repo *Repo) appearanceRecoveryEventFile(file *entity.File) bool {
	key, _ := appearanceArchiveKey(file.Path)
	return key != "" || repo.appearanceProtocolFileIgnored(file.Path) && appearanceEventContainerKey(file.Path) != ""
}

func (repo *Repo) mergeAppearanceRecoveryIndex(next, previous *entity.Index) (*entity.Index, error) {
	if previous == nil {
		return next, nil
	}
	retained := map[string]bool{}
	for _, id := range next.Files {
		retained[id] = true
	}
	merged := *next
	merged.Files = append([]string{}, next.Files...)
	for _, id := range previous.Files {
		file, err := repo.store.GetFile(id)
		if err != nil {
			return nil, err
		}
		if repo.appearanceRecoveryEventFile(file) && !retained[id] {
			merged.Files = append(merged.Files, id)
			merged.Size += file.Size
			retained[id] = true
		}
	}
	if len(merged.Files) == len(next.Files) {
		return next, nil
	}
	merged.ID = util.RandHash()
	merged.Count = len(merged.Files)
	merged.CheckIndexID = ""
	return &merged, nil
}

func (repo *Repo) sameAppearanceRootEvents(left, right *entity.Index) (bool, error) {
	if left == nil || right == nil {
		return left == right, nil
	}
	sets := []map[string]bool{{}, {}}
	for i, index := range []*entity.Index{left, right} {
		for _, id := range index.Files {
			file, err := repo.store.GetFile(id)
			if err != nil {
				return false, err
			}
			if repo.appearanceRecoveryEventFile(file) || file.Path == appearanceFormatPath {
				sets[i][id] = true
			}
		}
	}
	if len(sets[0]) != len(sets[1]) {
		return false, nil
	}
	for id := range sets[0] {
		if !sets[1][id] {
			return false, nil
		}
	}
	return true, nil
}

// rememberAppearanceRecoveryRoot 在普通快照引用中保留完整工作区，供本地清理和旧客户端还原后恢复。
func (repo *Repo) rememberAppearanceRecoveryRoot(index *entity.Index) error {
	if !repo.appearanceSyncEnabled {
		return nil
	}
	stat := newAppearanceTraffic()
	ctx := map[string]interface{}{}
	owned, err := repo.appearanceOwnsIndex(index, false, ctx, stat)
	if err != nil || !owned {
		return err
	}
	if err = repo.validateAppearanceRecoveryIndex(index, false, ctx, stat); err != nil {
		return err
	}
	previous, err := repo.loadAppearanceTagIndex(false, ctx, stat)
	if err != nil {
		return err
	}
	if previous != nil {
		if err = repo.validateAppearanceRecoveryIndex(previous, false, ctx, stat); err != nil {
			return err
		}
	}
	if index, err = repo.mergeAppearanceRecoveryIndex(index, previous); err != nil {
		return err
	}
	if same, compareErr := repo.sameAppearanceRootEvents(previous, index); compareErr != nil || same {
		return compareErr
	}
	if err = repo.store.PutIndex(index); err != nil {
		return err
	}
	abs := filepath.Join(repo.Path, "refs", "tags", AppearanceRecoveryTag)
	if err = os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
		return err
	}
	return gulu.File.WriteFileSafer(abs, []byte(index.ID), 0644)
}

// publishAppearanceRecoveryRoot 先补齐普通对象和索引，再更新恢复标签；latest 由调用方最后发布。
func (repo *Repo) publishAppearanceRecoveryRoot(index *entity.Index, context map[string]interface{}) (*TrafficStat, error) {
	stat := newAppearanceTraffic()
	if !repo.appearanceSyncEnabled {
		return stat, nil
	}
	if err := repo.validateAppearanceRecoveryIndex(index, false, context, stat); err != nil {
		return stat, err
	}
	// 目标快照中的本地忽略历史不得进入云端；之后只补回旧云端已经公开的引用。
	shared := *index
	shared.Files = nil
	for _, id := range index.Files {
		file, getErr := repo.store.GetFile(id)
		if getErr != nil {
			return stat, getErr
		}
		if file.Path != appearanceFormatPath && repo.appearanceProtocolFileIgnored(file.Path) {
			shared.Size -= file.Size
			continue
		}
		shared.Files = append(shared.Files, id)
	}
	if len(shared.Files) != len(index.Files) {
		shared.ID = util.RandHash()
		shared.Count = len(shared.Files)
		shared.CheckIndexID = ""
		index = &shared
	}
	previous, readStat, err := repo.readAppearanceRecoveryRoot(context)
	addAppearanceTraffic(stat, readStat)
	if err != nil {
		return stat, err
	}
	// 云端恢复根只合并已公开事件，绝不引用本地恢复根中尚未分享的包版本。
	if index, err = repo.mergeAppearanceRecoveryIndex(index, previous); err != nil {
		return stat, err
	}
	if err = repo.rememberAppearanceRecoveryRoot(index); err != nil {
		return stat, err
	}
	if same, compareErr := repo.sameAppearanceRootEvents(previous, index); compareErr != nil || same {
		return stat, compareErr
	}
	if err = repo.store.PutIndex(index); err != nil {
		return stat, err
	}
	files, err := repo.getFiles(index.Files)
	if err != nil {
		return stat, err
	}
	cloudFiles, refs, err := repo.cloud.GetRefsFiles()
	stat.APIGet += len(refs) + 1
	if errors.Is(err, cloud.ErrCloudObjectNotFound) || errors.Is(err, os.ErrNotExist) {
		cloudFiles, err = nil, nil
	}
	if err != nil {
		return stat, err
	}
	cloudFileSet := map[string]bool{}
	for _, id := range cloudFiles {
		cloudFileSet[id] = true
	}
	var uploadFiles []*entity.File
	var chunkFiles []*entity.File
	for _, file := range files {
		// 被忽略包只保留旧云端引用，不下载、修复或重新上传其历史负载。
		if file.Path != appearanceFormatPath && repo.appearanceProtocolFileIgnored(file.Path) {
			continue
		}
		if !cloudFileSet[file.ID] {
			uploadFiles = append(uploadFiles, file)
		}
		chunkFiles = append(chunkFiles, file)
	}
	uploadChunks, err := repo.cloud.GetChunks(repo.getChunks(chunkFiles))
	stat.APIGet++
	if err != nil {
		return stat, err
	}
	length, err := repo.uploadChunks(uploadChunks, context)
	stat.UploadBytes += length
	stat.UploadChunkCount += len(uploadChunks)
	stat.APIPut += len(uploadChunks)
	if err != nil {
		return stat, err
	}
	length, err = repo.uploadFiles(uploadFiles, context)
	stat.UploadBytes += length
	stat.UploadFileCount += len(uploadFiles)
	stat.APIPut += len(uploadFiles)
	if err != nil {
		return stat, err
	}
	length, err = repo.uploadIndex(index, context)
	stat.UploadBytes += length
	stat.UploadFileCount++
	stat.APIPut++
	if err != nil {
		return stat, err
	}
	length, err = repo.cloud.UploadBytes(path.Join("refs", "tags", AppearanceRecoveryTag), []byte(index.ID), true)
	stat.UploadBytes += length
	stat.UploadFileCount++
	stat.APIPut++
	return stat, err
}

func (repo *Repo) appearanceTagOwned(tag string, remote bool, context map[string]interface{}) (bool, error) {
	if !repo.appearanceSyncEnabled || !IsAppearanceRecoveryTag(tag) {
		return false, nil
	}
	stat := newAppearanceTraffic()
	index, err := repo.loadAppearanceTagIndex(remote, context, stat)
	if err != nil || index == nil {
		return false, err
	}
	return repo.appearanceOwnsIndex(index, remote, context, stat)
}

func (repo *Repo) guardAppearanceTag(tag string, remote bool, context map[string]interface{}) error {
	owned, err := repo.appearanceTagOwned(tag, remote, context)
	if err != nil {
		return err
	}
	if owned {
		return ErrAppearanceRecoveryTag
	}
	return nil
}

func (repo *Repo) lockAppearanceCloudTag(tag string, context map[string]interface{}) (func(), error) {
	if !repo.appearanceSyncEnabled || !IsAppearanceRecoveryTag(tag) {
		return func() {}, nil
	}
	if err := repo.tryLockCloud(repo.DeviceID, context); err != nil {
		return nil, err
	}
	return func() { repo.unlockCloud(context) }, nil
}

func (repo *Repo) appearanceCloudRepoStat() (*cloud.Stat, error) {
	stat, err := repo.cloud.GetStat()
	if err != nil || !repo.appearanceSyncEnabled || stat.Backup == nil || stat.Backup.Count == 0 {
		return stat, err
	}
	owned, err := repo.appearanceTagOwned(AppearanceRecoveryTag, true, map[string]interface{}{})
	if err != nil || !owned {
		return stat, err
	}
	copyStat := *stat
	copyBackup := *stat.Backup
	copyBackup.Count--
	copyStat.Backup = &copyBackup
	return &copyStat, nil
}
