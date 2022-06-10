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
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/restic/chunker"
)

// Repo 描述了逮虾户仓库。
type Repo struct {
	DataPath string // 数据文件夹的绝对路径，如：F:\\SiYuan\\data\\
	Path     string // 仓库的绝对路径，如：F:\\SiYuan\\history\\
	store    *Store // 仓库的存储

	ChunkPol     chunker.Pol // 文件分块多项式值
	ChunkMinSize uint        // 文件分块最小大小，单位：字节
	ChunkMaxSize uint        // 文件分块最大大小，单位：字节
}

func NewRepo(dataPath, repoPath string) (ret *Repo) {
	ret = &Repo{
		DataPath:     dataPath,
		ChunkPol:     chunker.Pol(0x3DA3358B4DC173), // TODO：固定多项式值副作用
		ChunkMinSize: 512 * 1024,                    // 分块最小 512KB
		ChunkMaxSize: 8 * 1024 * 1024,               // 分块最大 8MB
	}
	ret.store = NewStore(filepath.Join(repoPath, "objects"))
	return
}

func (repo *Repo) Checkout() (err error) {

	return
}

func (repo *Repo) Commit() (err error) {
	commit := &Commit{
		Parent:  "",
		Message: "",
		Created: time.Now().UnixMilli(),
	}
	err = filepath.Walk(repo.DataPath, func(path string, info os.FileInfo, err error) error {
		if nil != err {
			return io.EOF
		}
		if info.IsDir() || !info.Mode().IsRegular() {
			return nil
		}

		data, err := os.ReadFile(path)
		if nil != err {
			return err
		}

		size := info.Size()
		chnkr := chunker.NewWithBoundaries(bytes.NewReader(data), repo.ChunkPol, repo.ChunkMinSize, repo.ChunkMaxSize)
		buf := make([]byte, 8*1024*1024)
		var chunks []*Chunk
		var chunkHashes []string
		for {
			chnk, chnkErr := chnkr.Next(buf)
			if io.EOF == chnkErr {
				break
			}

			if nil != chnkErr {
				return chnkErr
			}

			chnkHash := Hash(chnk.Data)
			chunks = append(chunks, &Chunk{Hash: chnkHash, Body: chnk.Data})
			chunkHashes = append(chunkHashes, chnkHash)
		}

		for _, chunk := range chunks {
			err = repo.store.Put(chunk)
			if nil != err {
				return err
			}
		}

		relPath := strings.TrimPrefix(path, repo.DataPath)
		relPath = "/" + filepath.ToSlash(relPath)
		file := &File{Path: relPath, Size: size, Updated: info.ModTime().UnixMilli(), Body: chunkHashes}
		err = repo.store.Put(file)
		if nil != err {
			return err
		}

		commit.Body = append(commit.Body, file.ID())
		return nil
	})
	if nil != err {
		return
	}

	err = repo.store.Put(commit)
	if nil != err {
		return
	}

	return
}
