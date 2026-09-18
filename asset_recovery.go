package dejavu

import (
	"errors"
	"os"
	"sort"
	"time"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

// assetRecovery 保存需要调用方更新缓存和索引的路径，确认前不得丢弃
type assetRecovery struct {
	ID    string         `json:"id"`
	Files []*entity.File `json:"files"`
}

func (repo *Repo) recordAssetRecovery(pending *assetApply) error {
	files := map[string]*entity.File{}
	previous := repo.assetDownloads.state.Recovery
	if previous != nil {
		for _, file := range previous.Files {
			if !validAssetFile(file) {
				return ErrAssetDownloadState
			}
			files[file.Path] = file
		}
	}
	for _, file := range append(append([]*entity.File{}, pending.Upserts...), pending.Removes...) {
		files[file.Path] = file
	}
	if len(files) == 0 {
		return nil
	}
	recovery := &assetRecovery{ID: util.RandHash()}
	for _, file := range files {
		recovery.Files = append(recovery.Files, file)
	}
	sort.Slice(recovery.Files, func(i, j int) bool { return recovery.Files[i].Path < recovery.Files[j].Path })
	repo.assetDownloads.state.Recovery = recovery
	if err := repo.saveAssetState(); err != nil {
		repo.assetDownloads.state.Recovery = previous
		return err
	}
	return nil
}

// RecoverAssetDownloads 由同步调用方显式恢复落盘，再按实际文件状态返回待处理变更
func (repo *Repo) RecoverAssetDownloads(context map[string]interface{}) (string, *MergeResult, error) {
	lock.Lock()
	defer lock.Unlock()
	if err := repo.reloadAssetState(); err != nil {
		return "", nil, err
	}
	var recoverErr error
	if repo.assetDownloads != nil {
		recoverErr = repo.recoverAssetApply(context)
	}
	id, changes, err := repo.assetDownloadChanges()
	return id, changes, errors.Join(recoverErr, err)
}

// AssetDownloadChanges 只读取尚未确认的落盘变更，不触发恢复
func (repo *Repo) AssetDownloadChanges() (string, *MergeResult, error) {
	lock.Lock()
	defer lock.Unlock()
	if err := repo.reloadAssetState(); err != nil {
		return "", nil, err
	}
	return repo.assetDownloadChanges()
}

func (repo *Repo) assetDownloadChanges() (string, *MergeResult, error) {
	changes := &MergeResult{Time: time.Now()}
	if repo.assetDownloads == nil || repo.assetDownloads.state.Recovery == nil {
		return "", changes, nil
	}
	recovery := repo.assetDownloads.state.Recovery
	if recovery.ID == "" {
		return "", nil, ErrAssetDownloadState
	}
	for _, file := range recovery.Files {
		if !validAssetFile(file) {
			return "", nil, ErrAssetDownloadState
		}
		if _, err := os.Stat(repo.absPath(file.Path)); err == nil {
			changes.Upserts = append(changes.Upserts, file)
		} else if errors.Is(err, os.ErrNotExist) {
			changes.Removes = append(changes.Removes, file)
		} else {
			return "", nil, err
		}
	}
	return recovery.ID, changes, nil
}

// AcknowledgeAssetDownloadChanges 在调用方更新完成后确认同一批变更，失败时保留恢复记录
func (repo *Repo) AcknowledgeAssetDownloadChanges(id string) error {
	lock.Lock()
	defer lock.Unlock()
	if err := repo.checkAssetState(); err != nil {
		return err
	}
	if repo.assetDownloads == nil || repo.assetDownloads.state.Recovery == nil {
		return nil
	}
	previous := repo.assetDownloads.state.Recovery
	if previous.ID != id {
		return ErrAssetApplyPending
	}
	repo.assetDownloads.state.Recovery = nil
	if err := repo.saveAssetState(); err != nil {
		repo.assetDownloads.state.Recovery = previous
		return err
	}
	return nil
}
