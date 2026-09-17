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
	if _, err := repo.readAppearanceDiskArchive(key, expected, abs, false); err != nil {
		return nil, err
	}
	after, err := os.Stat(abs)
	if err != nil {
		return nil, err
	}
	if after.Size() != info.Size() || !after.ModTime().Equal(info.ModTime()) || !os.SameFile(info, after) {
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
