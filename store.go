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

func (store *Store) Put(chunk *Chunk) (err error) {
	if "" == chunk.Hash {
		chunk.Hash = Hash(chunk.Data)
	}
	dir, file := store.AbsPath(chunk.Hash)
	if err = os.MkdirAll(dir, 0755); nil != err {
		return
	}

	err = gulu.File.WriteFileSafer(file, chunk.Data, 0644)
	return
}

func (store *Store) Get(hash string) (chunk *Chunk, err error) {
	_, file := store.AbsPath(hash)
	data, err := os.ReadFile(file)
	if nil != err {
		return
	}
	chunk = &Chunk{Hash: hash, Data: data}
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

func (store *Store) AbsPath(hash string) (dir, file string) {
	dir = hash[0:2]
	file = hash[2:]
	dir = filepath.Join(store.Path, dir)
	file = filepath.Join(dir, file)
	return
}
