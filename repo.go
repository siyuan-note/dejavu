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
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/88250/gulu"
	"github.com/panjf2000/ants/v2"
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
		DataPath:     filepath.Clean(dataPath),
		Path:         filepath.Clean(repoPath),
		ChunkPol:     chunker.Pol(0x3DA3358B4DC173), // TODO：固定多项式值副作用
		ChunkMinSize: 512 * 1024,                    // 分块最小 512KB
		ChunkMaxSize: 8 * 1024 * 1024,               // 分块最大 8MB
	}
	ret.store = NewStore(filepath.Join(repoPath, "objects"))
	return
}

// Checkout 将仓库中的数据迁出到 repo 数据文件夹下。
func (repo *Repo) Checkout(id string) (err error) {
	index, err := repo.store.GetIndex(id)
	if nil != err {
		return
	}

	var files []*File
	err = filepath.Walk(repo.DataPath, func(path string, info os.FileInfo, err error) error {
		if nil != err {
			return io.EOF
		}
		if info.IsDir() || !info.Mode().IsRegular() {
			return nil
		}

		files = append(files, &File{
			Path:    repo.RelPath(path),
			Size:    info.Size(),
			Updated: info.ModTime().UnixMilli(),
		})
		return nil
	})
	if nil != err {
		return
	}

	var upserts, latestFiles []*File
	for _, f := range index.Files {
		var file *File
		file, err = repo.store.GetFile(f)
		if nil != err {
			return
		}
		latestFiles = append(latestFiles, file)
	}
	upserts = repo.Upsert(latestFiles, files)

	if 1 > len(upserts) {
		return
	}

	for _, f := range upserts {
		var file *File
		file, err = repo.store.GetFile(f.Hash)
		if nil != err {
			return err
		}

		var data []byte
		for _, c := range file.Chunks {
			var chunk *Chunk
			chunk, err = repo.store.GetChunk(c)
			if nil != err {
				return err
			}
			data = append(data, chunk.Data...)
		}

		p := filepath.Join(repo.DataPath, file.Path)
		dir := filepath.Dir(p)
		err = os.MkdirAll(dir, 0755)
		if nil != err {
			return
		}
		err = gulu.File.WriteFileSafer(p, data, 0644)
		if nil != err {
			return
		}

		updated := time.UnixMilli(file.Updated)
		err = os.Chtimes(p, updated, updated)
		if nil != err {
			return
		}
	}
	return
}

// Commit 将 repo 数据文件夹中的文件提交到仓库中。
func (repo *Repo) Commit() (ret *Index, err error) {
	var files []*File
	err = filepath.Walk(repo.DataPath, func(path string, info os.FileInfo, err error) error {
		if nil != err {
			return io.EOF
		}
		if info.IsDir() || !info.Mode().IsRegular() {
			return nil
		}

		files = append(files, &File{
			Path:    repo.RelPath(path),
			Size:    info.Size(),
			Updated: info.ModTime().UnixMilli(),
		})
		return nil
	})
	if nil != err {
		return
	}

	latest, err := repo.Latest()
	if nil != err {
		return
	}
	var upserts, latestFiles []*File
	if "" != latest.Parent {
		for _, f := range latest.Files {
			var file *File
			file, err = repo.store.GetFile(f)
			if nil != err {
				return
			}
			latestFiles = append(latestFiles, file)
		}
	}
	upserts = repo.Upsert(files, latestFiles)

	if 1 > len(upserts) {
		ret = latest
		return
	}

	waitGroup := &sync.WaitGroup{}
	var errs []error
	p, _ := ants.NewPoolWithFunc(runtime.NumCPU(), func(arg interface{}) {
		defer waitGroup.Done()
		err = repo.store.Put(arg.(Object))
		if nil != err {
			errs = append(errs, err)
		}
	})

	ret = &Index{
		Parent:  latest.Hash,
		Message: "",
		Created: time.Now().UnixMilli(),
	}
	for _, file := range upserts {
		var data []byte
		absPath := repo.AbsPath(file.Path)
		data, err = os.ReadFile(absPath)
		if nil != err {
			return
		}

		chnkr := repo.NewChunker(data)
		buf := make([]byte, 8*1024*1024)
		var chunks []*Chunk
		var chunkHashes []string
		for {
			chnk, chnkErr := chnkr.Next(buf)
			if io.EOF == chnkErr {
				break
			}
			if nil != chnkErr {
				err = chnkErr
				return
			}

			chnkHash := Hash(chnk.Data)
			chunks = append(chunks, &Chunk{Hash: chnkHash, Data: chnk.Data})
			chunkHashes = append(chunkHashes, chnkHash)
		}
		file.Chunks = chunkHashes

		for _, chunk := range chunks {
			waitGroup.Add(1)
			err = p.Invoke(chunk)
			if nil != err {
				return
			}
		}

		waitGroup.Add(1)
		err = p.Invoke(file)
		if nil != err {
			return
		}
	}

	for _, file := range files {
		ret.Files = append(ret.Files, file.ID())
	}
	waitGroup.Wait()
	p.Release()

	err = repo.store.Put(ret)
	if nil != err {
		return
	}
	if 0 < len(errs) {
		return nil, errs[0]
	}

	refs := filepath.Join(repo.Path, "refs")
	err = os.MkdirAll(refs, 0755)
	if nil != err {
		return
	}
	err = gulu.File.WriteFileSafer(filepath.Join(refs, "latest"), []byte(ret.ID()), 0644)
	return
}

func (repo *Repo) AbsPath(relPath string) string {
	return filepath.Join(repo.DataPath, relPath)
}

func (repo *Repo) RelPath(absPath string) string {
	absPath = filepath.Clean(absPath)
	return "/" + filepath.ToSlash(strings.TrimPrefix(absPath, repo.DataPath))
}

func (repo *Repo) NewChunker(data []byte) *chunker.Chunker {
	return chunker.NewWithBoundaries(bytes.NewReader(data), repo.ChunkPol, repo.ChunkMinSize, repo.ChunkMaxSize)
}
