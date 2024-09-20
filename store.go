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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/88250/gulu"
	"github.com/dgraph-io/ristretto"
	"github.com/klauspost/compress/zstd"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/encryption"
	"github.com/siyuan-note/logging"
)

var ErrNotFoundObject = errors.New("not found object")

// Store 描述了存储库。
type Store struct {
	Path   string // 存储库文件夹的绝对路径，如：F:\\SiYuan\\repo\\
	AesKey []byte

	compressEncoder *zstd.Encoder
	compressDecoder *zstd.Decoder
}

func NewStore(path string, aesKey []byte) (ret *Store, err error) {
	ret = &Store{Path: path, AesKey: aesKey}

	ret.compressEncoder, err = zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(false),
		zstd.WithWindowSize(512*1024))
	if nil != err {
		return
	}
	ret.compressDecoder, err = zstd.NewReader(nil,
		zstd.WithDecoderMaxMemory(16*1024*1024*1024))
	return
}

func (store *Store) Purge() (ret *entity.PurgeStat, err error) {
	logging.LogInfof("purging data repo [%s]", store.Path)

	objectsDir := filepath.Join(store.Path, "objects")
	if !gulu.File.IsDir(objectsDir) {
		logging.LogWarnf("objects dir [%s] is not a dir", objectsDir)
		return
	}

	entries, err := os.ReadDir(objectsDir)
	if nil != err {
		logging.LogErrorf("read objects dir [%s] failed: %s", objectsDir, err)
		return
	}

	objIDs := map[string]bool{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		dirName := entry.Name()
		dir := filepath.Join(objectsDir, dirName)
		objs, readErr := os.ReadDir(dir)
		if nil != readErr {
			err = readErr
			logging.LogErrorf("read objects dir [%s] failed: %s", dir, err)
			return
		}

		for _, obj := range objs {
			id := dirName + obj.Name()
			objIDs[id] = true
		}
	}

	indexIDs := map[string]bool{}
	indexesDir := filepath.Join(store.Path, "indexes")
	if gulu.File.IsDir(indexesDir) {
		entries, err = os.ReadDir(indexesDir)
		if nil != err {
			logging.LogErrorf("read indexes dir [%s] failed: %s", indexesDir, err)
			return
		}

		for _, entry := range entries {
			id := entry.Name()
			if 40 != len(id) {
				continue
			}

			indexIDs[id] = true
		}
	}

	refIndexIDs, err := store.readRefs()
	if nil != err {
		logging.LogErrorf("read refs failed: %s", err)
		return
	}

	unreferencedIndexIDs := map[string]bool{}
	for indexID := range indexIDs {
		if !refIndexIDs[indexID] {
			unreferencedIndexIDs[indexID] = true
		}
	}

	referencedObjIDs := map[string]bool{}
	for refID := range refIndexIDs {
		index, getErr := store.GetIndex(refID)
		if nil != getErr {
			err = getErr
			logging.LogErrorf("get index [%s] failed: %s", refID, err)
			return
		}

		for _, fileID := range index.Files {
			referencedObjIDs[fileID] = true
			file, getFileErr := store.GetFile(fileID)
			if nil != getFileErr {
				err = getFileErr
				logging.LogErrorf("get file [%s] failed: %s", fileID, err)
				return
			}

			for _, chunkID := range file.Chunks {
				referencedObjIDs[chunkID] = true
			}
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

	for unreferencedID := range unreferencedIDs {
		stat, statErr := store.Stat(unreferencedID)
		if nil != statErr {
			err = statErr
			logging.LogErrorf("stat [%s] failed: %s", unreferencedID, err)
			return
		}

		ret.Size += stat.Size()
		ret.Objects++

		if err = store.Remove(unreferencedID); nil != err {
			logging.LogErrorf("remove unreferenced object [%s] failed: %s", unreferencedID, err)
			return
		}
	}
	for unreferencedID := range unreferencedIndexIDs {
		indexPath := filepath.Join(store.Path, "indexes", unreferencedID)
		if err = os.RemoveAll(indexPath); nil != err {
			logging.LogErrorf("remove unreferenced index [%s] failed: %s", unreferencedID, err)
			return
		}
	}

	// 上面清理完索引了，最后再清理校验索引
	// Clear check index when purging data repo https://github.com/siyuan-note/siyuan/issues/9665
	checkIndexesDir := filepath.Join(store.Path, "check", "indexes")
	if gulu.File.IsDir(checkIndexesDir) {
		entries, err = os.ReadDir(checkIndexesDir)
		if nil != err {
			logging.LogErrorf("read check indexes dir [%s] failed: %s", checkIndexesDir, err)
		} else {
			for _, entry := range entries {
				id := entry.Name()
				if 40 != len(id) {
					continue
				}

				data, readErr := os.ReadFile(filepath.Join(checkIndexesDir, id))
				if nil != readErr {
					logging.LogErrorf("read check index [%s] failed: %s", id, readErr)
					continue
				}

				if data, readErr = store.compressDecoder.DecodeAll(data, nil); nil != readErr {
					logging.LogErrorf("decode check index [%s] failed: %s", id, readErr)
					continue
				}

				checkIndex := &entity.CheckIndex{}
				if readErr = gulu.JSON.UnmarshalJSON(data, checkIndex); nil != readErr {
					logging.LogErrorf("unmarshal check index [%s] failed: %s", id, readErr)
					continue
				}

				if _, statErr := os.Stat(filepath.Join(store.Path, "indexes", checkIndex.IndexID)); os.IsNotExist(statErr) {
					if removeErr := os.RemoveAll(filepath.Join(store.Path, "check", "indexes", checkIndex.ID)); nil != removeErr {
						logging.LogErrorf("remove check index [%s] failed: %s", checkIndex.ID, removeErr)
					}
				}
			}
		}
	}

	fileCache.Clear()
	indexCache.Clear()

	logging.LogInfof("purged data repo [%s], [%d] indexes, [%d] objects, [%d] bytes", store.Path, ret.Indexes, ret.Objects, ret.Size)
	return
}

func (store *Store) DeleteIndex(id string) (err error) {
	indexPath := filepath.Join(store.Path, "indexes", id)
	if err = os.RemoveAll(indexPath); nil != err {
		logging.LogErrorf("remove unreferenced index [%s] failed: %s", id, err)
		return
	}
	return
}

func (store *Store) PurgeV2() (ret *entity.PurgeStat, err error) {
	logging.LogInfof("purging data repo [%s]", store.Path)

	objIDs := map[string]bool{}

	collectObjErr := store.storeDirWalk("objects", func(_ int, entry fs.DirEntry, _ string) (cbErr error) {
		if !entry.IsDir() {
			return
		}
		dirName := entry.Name()
		dir := filepath.Join("objects", dirName)
		cbErr = store.storeDirWalk(dir, func(_ int, entry fs.DirEntry, _ string) (cbErr error) {
			id := dirName + entry.Name()
			objIDs[id] = true
			return
		})
		return
	})

	if nil != collectObjErr {
		err = fmt.Errorf("PurgeV2 collect object failed: %v", collectObjErr)
		return
	}

	indexIDs := map[string]bool{}

	collectIndexErr := store.storeDirWalk("indexes", func(index int, entry fs.DirEntry, _ string) (cbErr error) {
		id := entry.Name()
		if 40 != len(id) {
			return
		}
		indexIDs[id] = true
		return
	})

	if nil != collectIndexErr {
		err = fmt.Errorf("PurgeV2 collect Index failed: %v", collectIndexErr)
		return
	}

	refIndexIDs, err := store.readRefs()
	if nil != err {
		logging.LogErrorf("read refs failed: %s", err)
		return
	}

	unreferencedIndexIDs := map[string]bool{}
	for indexID := range indexIDs {
		if !refIndexIDs[indexID] {
			unreferencedIndexIDs[indexID] = true
		}
	}

	referencedObjIDs := map[string]bool{}
	for refID := range refIndexIDs {
		index, getErr := store.GetIndex(refID)
		if nil != getErr {
			err = getErr
			logging.LogErrorf("get index [%s] failed: %s", refID, err)
			return
		}

		for _, fileID := range index.Files {
			referencedObjIDs[fileID] = true
			file, getFileErr := store.GetFile(fileID)
			if nil != getFileErr {
				err = getFileErr
				logging.LogErrorf("get file [%s] failed: %s", fileID, err)
				return
			}

			for _, chunkID := range file.Chunks {
				referencedObjIDs[chunkID] = true
			}
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

	for unreferencedID := range unreferencedIDs {
		stat, statErr := store.Stat(unreferencedID)
		if nil != statErr {
			err = statErr
			logging.LogErrorf("stat [%s] failed: %s", unreferencedID, err)
			return
		}

		ret.Size += stat.Size()
		ret.Objects++

		if err = store.Remove(unreferencedID); nil != err {
			logging.LogErrorf("remove unreferenced object [%s] failed: %s", unreferencedID, err)
			return
		}
	}
	for unreferencedID := range unreferencedIndexIDs {
		indexPath := filepath.Join(store.Path, "indexes", unreferencedID)
		if err = os.RemoveAll(indexPath); nil != err {
			logging.LogErrorf("remove unreferenced index [%s] failed: %s", unreferencedID, err)
			return
		}
	}

	// 上面清理完索引了，最后再清理校验索引
	// Clear check index when purging data repo https://github.com/siyuan-note/siyuan/issues/9665

	checkIndexesDir := filepath.Join("check", "indexes")
	_ = store.storeDirWalk(checkIndexesDir, func(index int, entry fs.DirEntry, rootDir string) (cbErr error) {
		id := entry.Name()
		if 40 != len(id) {
			return
		}
		data, readErr := os.ReadFile(filepath.Join(rootDir, id))
		if nil != readErr {
			logging.LogErrorf("read check index [%s] failed: %s", id, readErr)
			return
		}

		if data, readErr = store.compressDecoder.DecodeAll(data, nil); nil != readErr {
			logging.LogErrorf("decode check index [%s] failed: %s", id, readErr)
			return
		}

		checkIndex := &entity.CheckIndex{}
		if readErr = gulu.JSON.UnmarshalJSON(data, checkIndex); nil != readErr {
			logging.LogErrorf("unmarshal check index [%s] failed: %s", id, readErr)
			return
		}

		if _, statErr := os.Stat(filepath.Join(store.Path, "indexes", checkIndex.IndexID)); os.IsNotExist(statErr) {
			if removeErr := os.RemoveAll(filepath.Join(store.Path, "check", "indexes", checkIndex.ID)); nil != removeErr {
				logging.LogErrorf("remove check index [%s] failed: %s", checkIndex.ID, removeErr)
			}
		}
		return
	})

	fileCache.Clear()
	indexCache.Clear()

	logging.LogInfof("purged data repo [%s], [%d] indexes, [%d] objects, [%d] bytes", store.Path, ret.Indexes, ret.Objects, ret.Size)
	return
}

func (store *Store) storeDirWalk(path string, callback func(index int, entry fs.DirEntry, rootDir string) (cbErr error)) (err error) {
	rootDir := filepath.Join(store.Path, path)
	if !gulu.File.IsDir(rootDir) {
		logging.LogWarnf("%s dir [%s] is not a dir", path, rootDir)
		err = errors.New(rootDir + "  is not a dir")
		return
	}

	entries, err := os.ReadDir(rootDir)
	if nil != err {
		logging.LogErrorf("read %s dir [%s] failed: %s", path, rootDir, err)
		return
	}
	for index, entry := range entries {
		cbErr := callback(index, entry, rootDir)
		if nil != cbErr {
			err = cbErr
			return
		}
	}
	return
}

func (store *Store) readRefs() (ret map[string]bool, err error) {
	ret = map[string]bool{}
	refsDir := filepath.Join(store.Path, "refs")
	if !gulu.File.IsDir(refsDir) {
		return
	}

	err = filepath.Walk(refsDir, func(path string, info os.FileInfo, err error) error {
		if nil != err {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if 42 < info.Size() {
			logging.LogWarnf("ref file [%s] is invalid", path)
			return nil
		}

		data, err := os.ReadFile(path)
		if nil != err {
			return err
		}

		content := strings.TrimSpace(string(data))
		if 40 != len(content) {
			logging.LogWarnf("ref file [%s] is invalid", path)
			return nil
		}

		ret[content] = true
		return nil
	})
	return
}

func (store *Store) PutIndex(index *entity.Index) (err error) {
	if "" == index.ID {
		return errors.New("invalid id")
	}
	dir, file := store.IndexAbsPath(index.ID)
	if err = os.MkdirAll(dir, 0755); nil != err {
		return errors.New("put index failed: " + err.Error())
	}

	data, err := gulu.JSON.MarshalJSON(index)
	if nil != err {
		return errors.New("put index failed: " + err.Error())
	}

	// Index 仅压缩，不加密
	data = store.compressEncoder.EncodeAll(data, nil)

	err = gulu.File.WriteFileSafer(file, data, 0644)
	if nil != err {
		return errors.New("put index failed: " + err.Error())
	}

	created := time.UnixMilli(index.Created)
	if err = os.Chtimes(file, created, created); nil != err {
		logging.LogWarnf("change index [%s] time failed: %s", index.ID, err.Error())
	}

	indexCache.Set(index.ID, index, int64(len(data)))
	return
}

func (store *Store) GetIndex(id string) (ret *entity.Index, err error) {
	cached, _ := indexCache.Get(id)
	if nil != cached {
		ret = cached.(*entity.Index)
		return
	}

	_, file := store.IndexAbsPath(id)
	var data []byte
	data, err = os.ReadFile(file)
	if nil != err {
		return
	}

	// Index 没有加密，直接解压
	data, err = store.compressDecoder.DecodeAll(data, nil)
	if nil == err {
		ret = &entity.Index{}
		err = gulu.JSON.UnmarshalJSON(data, ret)
	}
	if nil != err {
		return
	}

	indexCache.Set(id, ret, int64(len(data)))
	return
}

func (store *Store) PutFile(file *entity.File) (err error) {
	if "" == file.ID {
		return errors.New("invalid id")
	}
	dir, f := store.AbsPath(file.ID)
	if gulu.File.IsExist(f) {
		return
	}
	if err = os.MkdirAll(dir, 0755); nil != err {
		return errors.New("put failed: " + err.Error())
	}

	data, err := gulu.JSON.MarshalJSON(file)
	if nil != err {
		return errors.New("put file failed: " + err.Error())
	}
	if data, err = store.encodeData(data); nil != err {
		return
	}

	err = gulu.File.WriteFileSafer(f, data, 0644)
	if nil != err {
		return errors.New("put file failed: " + err.Error())
	}

	fileCache.Set(file.ID, file, int64(len(data)))
	return
}

func (store *Store) GetFile(id string) (ret *entity.File, err error) {
	cached, _ := fileCache.Get(id)
	if nil != cached {
		ret = cached.(*entity.File)
		return
	}

	_, file := store.AbsPath(id)
	data, err := os.ReadFile(file)
	if nil != err {
		return
	}
	if data, err = store.decodeData(data); nil != err {
		return
	}
	ret = &entity.File{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	if nil != err {
		ret = nil
		return
	}

	fileCache.Set(id, ret, int64(len(data)))
	return
}

func (store *Store) PutChunk(chunk *entity.Chunk) (err error) {
	if "" == chunk.ID {
		return errors.New("invalid id")
	}
	dir, file := store.AbsPath(chunk.ID)
	if gulu.File.IsExist(file) {
		return
	}

	if err = os.MkdirAll(dir, 0755); nil != err {
		return errors.New("put chunk failed: " + err.Error())
	}

	data := chunk.Data
	if data, err = store.encodeData(data); nil != err {
		return
	}

	err = gulu.File.WriteFileSafer(file, data, 0644)
	if nil != err {
		return errors.New("put chunk failed: " + err.Error())
	}
	return
}

func (store *Store) GetChunk(id string) (ret *entity.Chunk, err error) {
	_, file := store.AbsPath(id)
	data, err := os.ReadFile(file)
	if nil != err {
		return
	}
	if data, err = store.decodeData(data); nil != err {
		return
	}
	ret = &entity.Chunk{ID: id, Data: data}
	return
}

func (store *Store) Remove(id string) (err error) {
	_, file := store.AbsPath(id)
	err = os.RemoveAll(file)
	return
}

func (store *Store) Stat(id string) (stat os.FileInfo, err error) {
	_, file := store.AbsPath(id)
	stat, err = os.Stat(file)
	return
}

func (store *Store) IndexAbsPath(id string) (dir, file string) {
	dir = filepath.Join(store.Path, "indexes")
	file = filepath.Join(dir, id)
	return
}

func (store *Store) AbsPath(id string) (dir, file string) {
	dir, file = id[0:2], id[2:]
	dir = filepath.Join(store.Path, "objects", dir)
	file = filepath.Join(dir, file)
	return
}

func (store *Store) encodeData(data []byte) ([]byte, error) {
	data = store.compressEncoder.EncodeAll(data, nil)
	return encryption.AesEncrypt(data, store.AesKey)
}

func (store *Store) decodeData(data []byte) (ret []byte, err error) {
	ret, err = encryption.AesDecrypt(data, store.AesKey)
	if nil != err {
		return
	}
	ret, err = store.compressDecoder.DecodeAll(ret, nil)
	return
}

var fileCache, _ = ristretto.NewCache(&ristretto.Config{
	NumCounters: 200000,
	MaxCost:     1000 * 1000 * 32, // 1 个文件按 300 字节计算，32MB 大概可以缓存 10W 个文件实例
	BufferItems: 64,
})

var indexCache, _ = ristretto.NewCache(&ristretto.Config{
	NumCounters: 200000,
	MaxCost:     1000 * 1000 * 128, // 1 个文件按 300K 字节（大约 1.5W 个文件）计算，128MB 大概可以缓存 400 个索引
	BufferItems: 64,
})
