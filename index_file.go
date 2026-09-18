package dejavu

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/filelock"
)

// putIndexedFile 保留已有快照对象；时间戳复用导致内容冲突时，为磁盘文件分配新时间并重新索引。
func (repo *Repo) putIndexedFile(file *entity.File) error {
	if _, err := repo.store.Stat(file.ID); errors.Is(err, os.ErrNotExist) {
		return repo.store.PutFile(file)
	} else if err != nil {
		return err
	}
	previous, err := repo.store.GetFile(file.ID)
	if err != nil {
		return err
	}
	if previous.Path != file.Path || previous.SecUpdated() != file.SecUpdated() {
		return fmt.Errorf("%w: inconsistent indexed file: %s", ErrRepoFatal, file.Path)
	}
	if equalFileContent(previous, file) {
		return nil
	}
	// 外观协议文件的路径绑定内容，固定时间戳不可用于修复内容不一致。
	if strings.HasPrefix(file.Path, "/storage/appearance-v1/") {
		return fmt.Errorf("%w: inconsistent appearance object: %s", ErrRepoFatal, file.Path)
	}
	abs := repo.absPath(file.Path)
	filelock.Lock(abs)
	defer filelock.Unlock(abs)
	info, err := os.Stat(abs)
	if err != nil {
		return err
	}
	if info.Size() != file.Size || info.ModTime().UnixMilli() != file.Updated {
		return ErrIndexFileChanged
	}
	// 继续沿用路径和秒级时间生成 ID，避免覆盖历史对象，并兼容已有客户端。
	stamp := time.Now().Truncate(time.Second)
	for {
		candidate := entity.NewFile(file.Path, file.Size, stamp.UnixMilli())
		if _, err = repo.store.Stat(candidate.ID); errors.Is(err, os.ErrNotExist) {
			break
		} else if err != nil {
			return err
		}
		stamp = stamp.Add(time.Second)
	}
	if err = os.Chtimes(abs, stamp, stamp); err != nil {
		return err
	}
	return fmt.Errorf("%w: renewed reused timestamp: %s", ErrIndexFileChanged, file.Path)
}
