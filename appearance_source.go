package dejavu

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
	"github.com/siyuan-note/filelock"
)

// verifySourceAppearanceFile 在接受外观文件元数据前认证完整内容，已取得的分块供后续同步复用。
func (repo *Repo) verifySourceAppearanceFile(file *entity.File, stat *chunkDownloadStat) error {
	key, expected := appearanceArchiveKey(file.Path)
	if key == "" || !validAssetFile(file) {
		return fmt.Errorf("%w: invalid source appearance file [%s]", ErrRepoFatal, file.ID)
	}
	missing, err := repo.localNotFoundChunks(file.Chunks)
	if err != nil {
		return err
	}
	if len(missing) != 0 {
		found, queryErr := repo.chunkSource.HasChunks(missing)
		if queryErr != nil {
			return queryErr
		}
		for _, id := range missing {
			if !found[id] {
				return fmt.Errorf("source appearance chunk [%s] unavailable", id)
			}
		}
	}
	digest := sha256.New()
	var size int64
	for _, id := range file.Chunks {
		chunk, getErr := repo.sourceAppearanceChunk(id, stat)
		if getErr != nil {
			return getErr
		}
		if int64(len(chunk.Data)) > file.Size-size {
			return fmt.Errorf("%w: source appearance file [%s] size mismatch", ErrRepoFatal, file.ID)
		}
		size += int64(len(chunk.Data))
		digest.Write(chunk.Data)
	}
	if size != file.Size || hex.EncodeToString(digest.Sum(nil)) != expected {
		return fmt.Errorf("%w: source appearance file [%s] content mismatch", ErrRepoFatal, file.ID)
	}
	return nil
}

// sourceAppearanceChunk 按对象加锁，避免并发文件验证重复下载共享分块。
func (repo *Repo) sourceAppearanceChunk(id string, stat *chunkDownloadStat) (*entity.Chunk, error) {
	_, abs := repo.store.AbsPath(id)
	key := abs + ".appearance-source"
	filelock.Lock(key)
	defer filelock.Unlock(key)
	chunk, err := repo.store.GetChunk(id)
	if err == nil {
		if util.Hash(chunk.Data) != id {
			return nil, fmt.Errorf("%w: local appearance chunk [%s] hash mismatch", ErrRepoFatal, id)
		}
		return chunk, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	length, chunk, err := repo.downloadSourceChunk(id)
	if err != nil {
		return nil, err
	}
	if err = repo.store.PutChunk(chunk); err != nil {
		return nil, err
	}
	stat.PeerBytes += length
	stat.PeerCount++
	return chunk, nil
}
