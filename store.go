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
	"os"
	"path/filepath"

	"github.com/88250/gulu"
	"github.com/klauspost/compress/zstd"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/encryption"
)

var ErrNotFoundObject = errors.New("not found object")

// TODO: 增加缓存

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
	return
}

func (store *Store) GetIndex(id string) (ret *entity.Index, err error) {
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
	return
}

func (store *Store) GetFile(id string) (ret *entity.File, err error) {
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
	err = os.Remove(file)
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
