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
	"crypto/rand"
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/88250/gulu"
)

// Store 描述了存储库。
type Store struct {
	Path string // 存储库文件夹的绝对路径，如：F:\\SiYuan\\history\\objects\\
}

func NewStore(path string) *Store {
	return &Store{Path: path}
}

type Object interface {
	ID() string
}

func Hash(data []byte) string {
	return fmt.Sprintf("%x", sha1.Sum(data))
}

func RandHash() string {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if nil != err {
		return Hash([]byte(gulu.Rand.String(32)))
	}
	return Hash(b)
}

func (store *Store) Put(obj Object) (err error) {
	id := obj.ID()
	if "" == id {
		return errors.New("invalid id")
	}
	dir, file := store.AbsPath(id)
	if err = os.MkdirAll(dir, 0755); nil != err {
		return errors.New("put failed: " + err.Error())
	}

	data, err := gulu.JSON.MarshalJSON(obj)
	if nil != err {
		return errors.New("put failed: " + err.Error())
	}
	// TODO: 加密
	err = gulu.File.WriteFileSafer(file, data, 0644)
	if nil != err {
		return errors.New("put failed: " + err.Error())
	}
	return
}

func (store *Store) GetIndex(id string) (ret *Index, err error) {
	_, file := store.AbsPath(id)
	data, err := os.ReadFile(file)
	if nil != err {
		return
	}
	ret = &Index{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	return
}

func (store *Store) GetFile(id string) (ret *File, err error) {
	_, file := store.AbsPath(id)
	data, err := os.ReadFile(file)
	if nil != err {
		return
	}
	ret = &File{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	return
}

func (store *Store) GetChunk(id string) (ret *Chunk, err error) {
	_, file := store.AbsPath(id)
	data, err := os.ReadFile(file)
	if nil != err {
		return
	}
	ret = &Chunk{}
	err = gulu.JSON.UnmarshalJSON(data, ret)
	return
}

func (store *Store) Remove(hash string) (err error) {
	_, file := store.AbsPath(hash)
	err = os.Remove(file)
	return
}

func (store *Store) Stat(hash string) (stat os.FileInfo, err error) {
	_, file := store.AbsPath(hash)
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
