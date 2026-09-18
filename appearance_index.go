package dejavu

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/siyuan-note/dejavu/entity"
)

func (repo *Repo) snapshotFile(abs, p string, info os.FileInfo) (*entity.File, error) {
	file := entity.NewFile(p, info.Size(), info.ModTime().UnixMilli())
	key, expected := appearanceArchiveKey(p)
	if !repo.appearanceSyncEnabled || key == "" {
		return file, nil
	}
	if err := repo.checkAppearanceParents(abs); err != nil {
		return nil, err
	}
	// 安卓遍历信息不包含原生文件身份，且修改时间仅精确到毫秒。
	// 先核对遍历元数据，再用读取前后的原生信息检测文件替换及精确时间变化。
	source, err := os.Open(abs)
	if err != nil {
		return nil, err
	}
	// 通过句柄立即获取身份，避免 Windows 按路径延迟查询时读到替换后的文件。
	before, statErr := source.Stat()
	closeErr := source.Close()
	if statErr != nil {
		return nil, statErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if before.Size() != info.Size() || before.ModTime().UnixMilli() != info.ModTime().UnixMilli() {
		return nil, fmt.Errorf("%w: appearance file changed during scan: %s", ErrIndexFileChanged, p)
	}
	if _, err := repo.readAppearanceDiskArchive(key, expected, abs, false); err != nil {
		return nil, err
	}
	after, err := os.Stat(abs)
	if err != nil {
		return nil, err
	}
	if after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) || !os.SameFile(before, after) {
		return nil, fmt.Errorf("%w: appearance file changed during scan: %s", ErrIndexFileChanged, p)
	}
	return file, nil
}

func (repo *Repo) verifyAppearanceFileIdentity(file *entity.File) error {
	key, expected := appearanceArchiveKey(file.Path)
	if !repo.appearanceSyncEnabled || key == "" {
		return nil
	}
	digest := sha256.New()
	for _, id := range file.Chunks {
		chunk, err := repo.store.GetChunk(id)
		if err != nil {
			return err
		}
		digest.Write(chunk.Data)
	}
	if hex.EncodeToString(digest.Sum(nil)) != expected {
		return fmt.Errorf("%w: appearance file changed after scan: %s", ErrIndexFileChanged, file.Path)
	}
	return nil
}
