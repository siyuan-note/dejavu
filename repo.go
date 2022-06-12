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
	"github.com/siyuan-note/dejavu/entity"
	"github.com/siyuan-note/dejavu/util"
)

// Repo 描述了逮虾户仓库。
type Repo struct {
	DataPath string // 数据文件夹的绝对路径，如：F:\\SiYuan\\data\\
	Path     string // 仓库的绝对路径，如：F:\\SiYuan\\history\\
	store    *Store // 仓库的存储

	ChunkPol     chunker.Pol // 文件分块多项式值
	ChunkMinSize int64       // 文件分块最小大小，单位：字节
	ChunkMaxSize int64       // 文件分块最大大小，单位：字节
}

func NewRepo(dataPath, repoPath string, aesKey []byte) (ret *Repo, err error) {
	ret = &Repo{
		DataPath:     filepath.Clean(dataPath),
		Path:         filepath.Clean(repoPath),
		ChunkPol:     chunker.Pol(0x3DA3358B4DC173), // TODO：固定多项式值副作用
		ChunkMinSize: 512 * 1024,                    // 分块最小 512KB
		ChunkMaxSize: 8 * 1024 * 1024,               // 分块最大 8MB
	}
	ret.DataPath = filepath.Clean(ret.DataPath)
	if !strings.HasSuffix(ret.DataPath, string(os.PathSeparator)) {
		ret.DataPath += string(os.PathSeparator)
	}
	if !strings.HasSuffix(ret.Path, string(os.PathSeparator)) {
		ret.Path += string(os.PathSeparator)
	}
	storePath := filepath.Join(repoPath, "objects") + string(os.PathSeparator)
	ret.store = NewStore(storePath, aesKey)
	return
}

// Checkout 将仓库中的数据迁出到 repo 数据文件夹下。
func (repo *Repo) Checkout(id string) (err error) {
	index, err := repo.store.GetIndex(id)
	if nil != err {
		return
	}

	if err = os.MkdirAll(repo.DataPath, 0755); nil != err {
		return
	}
	var files []*entity.File
	err = filepath.Walk(repo.DataPath, func(path string, info os.FileInfo, err error) error {
		if nil != err {
			return io.EOF
		}
		if info.IsDir() || !info.Mode().IsRegular() {
			return nil
		}

		files = append(files, &entity.File{
			Path:    repo.RelPath(path),
			Size:    info.Size(),
			Updated: info.ModTime().UnixMilli(),
		})
		return nil
	})
	if nil != err {
		return
	}

	var upserts, removes, latestFiles []*entity.File
	for _, f := range index.Files {
		var file *entity.File
		file, err = repo.store.GetFile(f)
		if nil != err {
			return
		}
		latestFiles = append(latestFiles, file)
	}
	upserts, removes = repo.DiffUpsertRemove(latestFiles, files)
	if 1 > len(upserts) && 1 > len(removes) {
		return
	}

	for _, f := range upserts {
		var file *entity.File
		file, err = repo.store.GetFile(f.Hash)
		if nil != err {
			return err
		}

		var data []byte
		for _, c := range file.Chunks {
			var chunk *entity.Chunk
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

	for _, f := range removes {
		p := repo.AbsPath(f.Path)
		if err = os.Remove(p); nil != err {
			return
		}
	}
	return
}

// Commit 将 repo 数据文件夹中的文件提交到仓库中。
func (repo *Repo) Commit(message string) (ret *entity.Index, err error) {
	var files []*entity.File
	err = filepath.Walk(repo.DataPath, func(path string, info os.FileInfo, err error) error {
		if nil != err {
			return io.EOF
		}
		if info.IsDir() || !info.Mode().IsRegular() {
			return nil
		}

		files = append(files, &entity.File{
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
	var upserts, removes, latestFiles []*entity.File
	if "" != latest.Parent {
		for _, f := range latest.Files {
			var file *entity.File
			file, err = repo.store.GetFile(f)
			if nil != err {
				return
			}
			latestFiles = append(latestFiles, file)
		}
	}
	upserts, removes = repo.DiffUpsertRemove(files, latestFiles)
	if 1 > len(upserts) && 1 > len(removes) {
		ret = latest
		return
	}

	waitGroup := &sync.WaitGroup{}
	var errs []error
	p, _ := ants.NewPoolWithFunc(runtime.NumCPU(), func(arg interface{}) {
		defer waitGroup.Done()
		var putErr error
		switch obj := arg.(type) {
		case *entity.Chunk:
			putErr = repo.store.PutChunk(obj)
		case *entity.File:
			putErr = repo.store.PutFile(obj)
		case *entity.Index:
			putErr = repo.store.PutIndex(obj)
		}

		if nil != putErr {
			errs = append(errs, putErr)
		}
	})

	ret = &entity.Index{
		Hash:    util.RandHash(),
		Parent:  latest.Hash,
		Message: message,
		Created: time.Now().UnixMilli(),
	}
	for _, file := range upserts {
		absPath := repo.AbsPath(file.Path)
		chunks, hashes, chunkErr := repo.fileChunks(absPath)
		if nil != chunkErr {
			err = chunkErr
			return
		}
		file.Chunks = hashes

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
		ret.Size += file.Size
	}
	waitGroup.Wait()
	p.Release()

	err = repo.store.PutIndex(ret)
	if nil != err {
		return
	}
	if 0 < len(errs) {
		return nil, errs[0]
	}

	err = repo.UpdateLatest(ret.ID())
	return
}

func (repo *Repo) AbsPath(relPath string) string {
	return filepath.Join(repo.DataPath, relPath)
}

func (repo *Repo) RelPath(absPath string) string {
	absPath = filepath.Clean(absPath)
	return "/" + filepath.ToSlash(strings.TrimPrefix(absPath, repo.DataPath))
}

func (repo *Repo) fileChunks(absPath string) (chunks []*entity.Chunk, chunkHashes []string, err error) {
	info, statErr := os.Stat(absPath)
	if nil != statErr {
		err = statErr
		return
	}

	if repo.ChunkMinSize > info.Size() {
		data, readErr := os.ReadFile(absPath)
		if nil != readErr {
			err = readErr
			return
		}
		chnkHash := util.Hash(data)
		chunks = append(chunks, &entity.Chunk{Hash: chnkHash, Data: data})
		chunkHashes = append(chunkHashes, chnkHash)
		return
	}

	reader, err := os.OpenFile(absPath, os.O_RDONLY, 0644)
	if nil != err {
		return
	}
	chnkr := chunker.NewWithBoundaries(reader, repo.ChunkPol, uint(repo.ChunkMinSize), uint(repo.ChunkMaxSize))
	buf := make([]byte, 8*1024*1024)
	for {
		chnk, chnkErr := chnkr.Next(buf)
		if io.EOF == chnkErr {
			break
		}
		if nil != chnkErr {
			err = chnkErr
			return
		}

		chnkHash := util.Hash(chnk.Data)
		chunks = append(chunks, &entity.Chunk{Hash: chnkHash, Data: chnk.Data})
		chunkHashes = append(chunkHashes, chnkHash)
	}
	return
}
