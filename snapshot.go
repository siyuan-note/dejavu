package dejavu

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/siyuan-note/dejavu/entity"
)

// IndexWithResult 在索引锁内判断是否创建了新快照；fullCheck 校验所有已落盘文件。
func (repo *Repo) IndexWithResult(memo string, checkChunks bool, context map[string]interface{}, fullCheck ...bool) (*entity.Index, bool, error) {
	lock.Lock()
	defer lock.Unlock()
	latest, err := repo.Latest()
	if err != nil && !errors.Is(err, ErrNotFoundIndex) {
		return nil, false, err
	}
	index, err := repo.index(memo, checkChunks, context, fullCheck...)
	if err != nil {
		return nil, false, err
	}
	return index, latest == nil || latest.ID != index.ID, nil
}

// CheckSnapshot 复用已确认的本机文件属性，必要时读取内容；不下载资源、不修复引用或写入仓库。
func (repo *Repo) CheckSnapshot() (bool, error) {
	lock.Lock()
	defer lock.Unlock()
	if err := repo.checkAssetState(); err != nil {
		return false, err
	}
	files, stamps, err := repo.walkSnapshotFiles(nil)
	if err != nil {
		return false, err
	}
	// 正式索引会先物化被忽略的资源，再停止跟踪；预检查仅排除这些逻辑资源。
	matcher := repo.ignoreMatcher()
	if repo.assetDownloads != nil {
		physical := filesByPath(files)
		for p, file := range repo.assetDownloads.state.Deferred {
			if physical[p] == nil && !matcher.MatchesPath(p) {
				files = append(files, file)
			}
		}
	}
	if len(files) == 0 {
		return false, ErrEmptyIndex
	}
	data, err := os.ReadFile(filepath.Join(repo.Path, "refs", "latest"))
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	id := string(data)
	if !validSnapshotID(id) {
		return false, fmt.Errorf("%w: invalid latest index ID", ErrRepoFatal)
	}
	latest, err := repo.store.GetIndex(id)
	if err != nil {
		return false, err
	}
	if latest.ID != id || !latest.VerifyAESKey(repo.store.AesKey) {
		return false, fmt.Errorf("%w: invalid snapshot index", ErrRepoFatal)
	}
	// 读取实际文件对象，避免缓存文件缺失或损坏时触发缓存删除，也保留认证读取。
	latestFiles, err := repo.getFiles(latest.Files)
	if err != nil {
		return false, err
	}
	if err = repo.loadFileState(latest.ID); err != nil {
		return false, err
	}
	upserts, removes, err := repo.diffLocalFiles(files, latestFiles, stamps, false)
	if err != nil {
		return false, err
	}
	return len(upserts) != 0 || len(removes) != 0, nil
}

func validSnapshotID(id string) bool {
	if len(id) != 40 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

// SetSnapshotMemo 仅更新本地索引；复制缓存对象，写入失败时保留原备注。
func (repo *Repo) SetSnapshotMemo(id, memo string) error {
	lock.Lock()
	defer lock.Unlock()
	if !validSnapshotID(id) {
		return errors.New("invalid snapshot ID")
	}
	index, err := repo.store.GetIndex(id)
	if err != nil {
		return err
	}
	if index.ID != id || !index.VerifyAESKey(repo.store.AesKey) {
		return fmt.Errorf("%w: invalid snapshot index", ErrRepoFatal)
	}
	updated := *index
	updated.Memo = memo
	return repo.store.PutIndex(&updated)
}
