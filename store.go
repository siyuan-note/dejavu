// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
//
// DejaVu is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//         http://license.coscl.org.cn/MulanPSL2
//
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
//
// See the Mulan PSL v2 for more details.

package dejavu

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/88250/gulu"
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/encryption"
)

// Store 描述了存储库。
type Store struct {
	Path   string // 存储库文件夹的绝对路径，如：F:\\SiYuan\\history\\objects\\
	AesKey []byte
}

func NewStore(path string, aesKey []byte) *Store {
	return &Store{Path: path, AesKey: aesKey}
}

type Object interface {
	ID() string
}

func (store *Store) PutChunk(chunk *entity.Chunk) (err error) {
	id := chunk.ID()
	if "" == id {
		return errors.New("invalid id")
	}
	dir, file := store.AbsPath(id)
	if err = os.MkdirAll(dir, 0755); nil != err {
		return errors.New("put chunk failed: " + err.Error())
	}

	data := chunk.Data
	data, err = store.encryptData(data)
	if nil != err {
		return
	}

	err = gulu.File.WriteFileSafer(file, data, 0644)
	if nil != err {
		return errors.New("put chunk failed: " + err.Error())
	}
	return
}

func (store *Store) PutFile(file *entity.File) (err error) {
	id := file.ID()
	if "" == id {
		return errors.New("invalid id")
	}
	dir, f := store.AbsPath(id)
	if err = os.MkdirAll(dir, 0755); nil != err {
		return errors.New("put failed: " + err.Error())
	}

	data, err := gulu.JSON.MarshalJSON(file)
	if nil != err {
		return errors.New("put file failed: " + err.Error())
	}
	data, err = store.encryptData(data)
	if nil != err {
		return
	}

	err = gulu.File.WriteFileSafer(f, data, 0644)
	if nil != err {
		return errors.New("put file failed: " + err.Error())
	}
	return
}

func (store *Store) PutIndex(index *entity.Index) (err error) {
	id := index.ID()
	if "" == id {
		return errors.New("invalid id")
	}
	dir, file := store.AbsPath(id)
	if err = os.MkdirAll(dir, 0755); nil != err {
		return errors.New("put index failed: " + err.Error())
	}

	data, err := gulu.JSON.MarshalJSON(index)
	if nil != err {
		return errors.New("put index failed: " + err.Error())
	}

	buf := &bytes.Buffer{}
	gz := gzip.NewWriter(buf)
	if _, err = gz.Write(data); nil != err {
		return errors.New("put index failed: " + err.Error())
	}
	if err = gz.Close(); nil != err {
		return errors.New("put index failed: " + err.Error())
	}
	data = buf.Bytes()

	if data, err = store.encryptData(data); nil != err {
		return
	}

	err = gulu.File.WriteFileSafer(file, data, 0644)
	if nil != err {
		return errors.New("put index failed: " + err.Error())
	}
	return
}

func (store *Store) GetIndex(id string) (ret *entity.Index, err error) {
	_, file := store.AbsPath(id)
	data, err := os.ReadFile(file)
	if nil != err {
		return
	}
	data, err = store.decryptData(data)
	if nil != err {
		return
	}

	buf := bytes.NewReader(data)
	reader, err := gzip.NewReader(buf)
	if nil != err {
		return
	}
	data, err = io.ReadAll(reader)
	if nil != err {
		return
	}

	ret = &entity.Index{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	return
}

func (store *Store) GetFile(id string) (ret *entity.File, err error) {
	_, file := store.AbsPath(id)
	data, err := os.ReadFile(file)
	if nil != err {
		return
	}
	data, err = store.decryptData(data)
	if nil != err {
		return
	}
	ret = &entity.File{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	return
}

func (store *Store) GetChunk(id string) (ret *entity.Chunk, err error) {
	_, file := store.AbsPath(id)
	data, err := os.ReadFile(file)
	if nil != err {
		return
	}
	data, err = store.decryptData(data)
	if nil != err {
		return
	}
	ret = &entity.Chunk{Hash: id, Data: data}
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

func (store *Store) AbsPath(id string) (dir, file string) {
	dir = id[0:2]
	file = id[2:]
	dir = filepath.Join(store.Path, dir)
	file = filepath.Join(dir, file)
	return
}

func (store *Store) encryptData(data []byte) ([]byte, error) {
	return encryption.AesEncrypt(data, store.AesKey)
}

func (store *Store) decryptData(data []byte) ([]byte, error) {
	return encryption.AesDecrypt(data, store.AesKey)
}
